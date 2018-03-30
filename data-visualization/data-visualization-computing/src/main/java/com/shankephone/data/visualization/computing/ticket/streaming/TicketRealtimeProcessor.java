package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.City;
import com.shankephone.data.cache.CityCache;
import com.shankephone.data.cache.Station;
import com.shankephone.data.cache.StationCache;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

/**
 * 实时交易记录处理
 * @author fengql
 * @version 2018年2月5日 下午2:53:13
 */
public class TicketRealtimeProcessor implements SparkStreamingProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(TicketRealtimeProcessor.class);
	private final static boolean debug = false;

	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args) {
		rdd.filter(f->{
			return filterTicketData(f);
		}).foreach(f->{
			processData(f);
		});
	}	
	
	/**
	 * 实时处理与推送
	 * @param value
	 */
	private static void processData(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		if(cityCode == null || "".equals(cityCode)){
			logger.error("城市代码为空！忽略了该数据：" + value);
			return;
		}
		String type = null;
		Integer ticketNum = 0;
		String datetime = null;
		//进站、出站
		String entryStation = null;
		String exitStation = null;
		String entryCode = null;
		String exitCode = null;
		JSONObject json = new JSONObject();
		if ("SKP:ORDER_INFO".equals(tableName) ) {
			entryStation = columns.getString("TICKET_PICKUP_STATION_CODE");
			exitStation = columns.getString("TICKET_GETOFF_STATION_CODE");
			entryCode = entryStation;
			exitCode = exitStation;
			String productCode = columns.getString("ORDER_PRODUCT_CODE");
			String productCodePre = productCode.split("_")[0];
			//单程票
			if ("DCP".equals(productCodePre) || "TVIP".equals(productCodePre) || "GZH".equals(productCodePre)) { 				//单程票
				type = Constants.ORDER_TYPE_DCP;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				datetime = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			} 
			//圈存
			if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	//  长沙充值
				type = Constants.ORDER_TYPE_NFC;
				exitStation = "";
				ticketNum = 1;
				datetime = columns.getString("TOPUP_TOPUP_DATE");
			} 
			//珠海有轨（无站点信息）
			if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		
				type = Constants.ORDER_TYPE_XFHX;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				datetime = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			}
		} 
		//咖啡
		if ("COFFEE:T_TASTE_ORDER".equals(tableName)) {	
			entryStation = columns.getString("PARTNER_ID");
			exitStation = "";
			entryCode = entryStation;
			exitCode = "";
			type = Constants.ORDER_TYPE_COFFEE;
			ticketNum = 1;
			datetime = columns.getString("UPDATE_DATE");
		}
		//先付后享 非珠海（乘车码）
		if ("SKP:SJT_QR_CODE".equals(tableName)) { 	
			entryStation = columns.getString("SJT_ENTRY_STATION_CODE");
			exitStation = columns.getString("SJT_EXIT_STATION_CODE");
			entryCode = entryStation;
			exitCode = exitStation;
			type = Constants.ORDER_TYPE_XFHX;
			ticketNum = 1;
			datetime = columns.getString("SJT_SALE_DATE");
		} 
		//先享后付
		if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){
			String stationCode = columns.getString("TRANS_STATION_CODE");
			type = Constants.ORDER_TYPE_XXHF;
			String trxType = columns.getString("TRX_TYPE");
			if(Constants.XXHF_TRX_TYPE_ENTRY.equals(trxType)){
				entryStation = stationCode;
				entryCode = entryStation;
			} 
			if(Constants.XXHF_TRX_TYPE_EXIT.equals(trxType)){
				exitStation = stationCode;
				exitCode = stationCode;
			} else if(Constants.XXHF_TRX_TYPE_UNUSUAL.equals(trxType)){
				exitStation = stationCode;
			}
			ticketNum = 1;
			String dt = columns.getString("TRANS_DATE") + columns.getString("TRANS_TIME");
			datetime = DateUtils.formatDateTime(DateUtils.parseDate(dt,"yyyyMMddHHmmss"));
		}
		Station start = StationCache.getInstance().get(cityCode + "_" + entryStation);
		if(start != null){
			entryStation = start.getStationNameZh();
		} 
		Station end = StationCache.getInstance().get(cityCode + "_" + exitStation);
		if(end != null){
			exitStation = end.getStationNameZh(); 
		} 
		//圈存设置显示的实时名称
		if(Constants.ORDER_TYPE_NFC.equals(type)){
			entryStation = "圈存";
		} else if(Constants.ORDER_TYPE_COFFEE.equals(type)){
			//咖啡设置显示的店名
			entryStation = Constants.coffeeShopMaps.get(entryStation);
		} else if(start == null){
			entryStation = "";
		} else if(end == null){
			exitStation = "";
		}
		if(debug){
			if("0000".equals(entryStation) || "0000".equals(exitStation)){
				logger.error("错误站点数据：" + value.toJSONString());
			}
		}
		
		//站点处理
		if(!Constants.ORDER_TYPE_COFFEE.equals(type) && !Constants.ORDER_TYPE_NFC.equals(type)){
			//无进站
			if(entryStation == null || "".equals(entryStation.trim())){
				if(exitStation == null || "".equals(exitStation.trim())){
					//无进站无出站
					//logger.error("无进、出站信息：" + value.toJSONString());
				} else {
					//无进站有出站
					exitStation = exitStation + "(出站)";
				}
			} else {
				//有进站
				if(exitStation == null || "".equals(exitStation.trim())){
					//有进站无出站
					entryStation = entryStation + "(进站)";
				} else {
					//有进站和出站
				}
			}
		}
		//先享后付，补票
		if(!Constants.ORDER_TYPE_XXHF.equals(type)){
			String trxType = columns.getString("TRX_TYPE");
			if(Constants.XXHF_TRX_TYPE_UNUSUAL.equals(trxType)){
				if(exitStation != null && !"".equals(exitStation)){
					exitStation = exitStation + "(补票)";
				}
			}
		}
		RedissonClient redisson =  RedisUtils.getRedissonClient();
		json.put("entryStation", entryStation);
		json.put("exitStation", exitStation);
		if(datetime == null){
			if(debug){
				logger.error("获取时间为空：" + value); 
			}
			return ;
		}
		City city = CityCache.getInstance().get(cityCode);
		//json.put("orderNo", columns.getString("ORDER_NO"));
		json.put("cityCode", cityCode);
		json.put("cityName", city.getName());
		json.put("time", datetime);
		json.put("ticketNum", ticketNum);
		json.put("type", type);
		json.put("ticketType", Constants.orderTypeMap.get(type));
		json.put("entryCode", entryCode);
		json.put("exitCode", exitCode);
		
		//发布时间，小于缓存时间则不发布
		boolean result = LastTimeUtils.canPublishLastTime(datetime);
		if(result){
			publishLastTime(datetime, redisson);
		}
		String day = DateUtils.formatDate(DateUtils.parseDate(datetime,"yyyy-MM-dd HH:mm:ss")); 
		//发布数据
		boolean res = LastTimeUtils.canPublishData(day);
		if(res){
			redisson.getTopic(Constants.REDIS_TOPIC_TICKET_RECORD + "0000").publish(json.toJSONString());
			redisson.getTopic(Constants.REDIS_TOPIC_TICKET_RECORD + cityCode).publish(json.toJSONString());
		}
	}

	/**
	 * 发布数据时间
	 * @param datetime
	 * @param redisson
	 */
	private static void publishLastTime(String datetime,
			RedissonClient redisson) {
		RBucket<String> bucket = redisson.getBucket("lastTime");
		//缓存时间
		bucket.set(datetime);
		JSONObject timestampJson = new JSONObject();
		timestampJson.put("lastTime", datetime);
		//发布时间
		redisson.getTopic("ticket:lastTime").publish(timestampJson.toJSONString());
	}
	
	public static boolean filterTicketData(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		if(tableName == null || columns == null) {
			return false;
		}
		if (StringUtils.isNotBlank(columns.getString("CITY_CODE")) 
				&& ("SKP:ORDER_INFO".equals(tableName) ||"SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName) 
				||	"COFFEE:T_TASTE_ORDER".equals(tableName) || "SKP:SJT_QR_CODE".equals(tableName) 
				|| "SKP:TR_ORDER_THRID".equals(tableName))) {
			return filterData(tableName,columns,value);
		}
		return false;
	}
	public static boolean filterData(String tableName,JSONObject columns,JSONObject value){
		String cityCode = columns.getString("CITY_CODE");
		RedissonClient redisson = RedisUtils.getRedissonClient();
		if (value.containsKey("changes") && ("SKP:ORDER_INFO".equals(tableName) || "COFFEE:T_TASTE_ORDER".equals(tableName))) {
			//过滤单程票、长沙充值nfc、先付后享-珠海有轨
			return filterPartOne(tableName,columns,value,cityCode,redisson);
		} else {
			//过滤用户数据、先付后享-非珠海（乘车码）、先享后付
			return filterPartTwo(tableName,columns,value,cityCode,redisson);
		}
	}
	//过滤单程票、长沙充值nfc、先付后享-珠海有轨
	public static boolean filterPartOne(String tableName,JSONObject columns,JSONObject value,String cityCode,RedissonClient redisson){
		boolean statusFlag = false;
		boolean preStatusFlag = false;
		boolean ticketFlag = false;
		boolean dateFlag = false;
		JSONObject changes = value.getJSONObject("changes");
		if ("SKP:ORDER_INFO".equals(tableName)) {
			String productCode = columns.getString("ORDER_PRODUCT_CODE");
			if (!StringUtils.isNotBlank(productCode)) {
				return false;
			}
			if (productCode.startsWith("DCP_") || productCode.startsWith("TVIP_") || productCode.startsWith("GZH_")) { 				
				//单程票
				String entryStationCode = columns.getString("TICKET_PICKUP_STATION_CODE");
				String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
				String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
				statusFlag = "5".equals(columns.getString("TICKET_ORDER_STATUS"));
				preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
				ticketFlag = StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(ticketNum);
				dateFlag = StringUtils.isNotBlank(time);
			} else if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	
				//长沙充值
				statusFlag = "5".equals(columns.getString("TOPUP_ORDER_STATUS"));
				preStatusFlag = !"5".equals(changes.getString("TOPUP_ORDER_STATUS"));
				ticketFlag = true;
				dateFlag = StringUtils.isNotBlank(columns.getString("TOPUP_TOPUP_DATE"));
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		
				//珠海有轨（没有站点信息）
				statusFlag = "5".equals(columns.getString("TICKET_ORDER_STATUS"));
				preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
				ticketFlag = StringUtils.isNotBlank(columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM"));
				dateFlag = StringUtils.isNotBlank(columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE"));
			}
		} else {//咖啡  
			statusFlag = "4".equals(columns.getString("ORDER_STATE"));
			preStatusFlag = !"4".equals(changes.getString("ORDER_STATE"));
			ticketFlag = true;
			dateFlag = StringUtils.isNotBlank(columns.getString("UPDATE_DATE"));
		}
		return (statusFlag && preStatusFlag && ticketFlag && dateFlag);
	}
	//过滤用户数据、先付后享-非珠海（乘车码）、先享后付
	public static boolean filterPartTwo(String tableName,JSONObject columns,JSONObject value,String cityCode,RedissonClient redisson){
		if ("SKP:TR_ORDER_THRID".equals(tableName)) {     //用户数据
			String payDate = columns.getString("PAY_TIME").split(" ")[0];
			String payAccount = columns.getString("PAY_ACCOUNT");
			String state = columns.getString("STATE");
			if ("2".equals(state) && StringUtils.isNotBlank(payAccount) && StringUtils.isNotBlank(payDate)) {
				return true;
			}
		} else if ("SKP:SJT_QR_CODE".equals(tableName)) { 	
			//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
			String saleDate = columns.getString("SJT_SALE_DATE");
			String entryStationCode = columns.getString("SJT_ENTRY_STATION_CODE");
			String sjt_status = columns.getString("SJT_STATUS");
			if (sjt_status != null && !"03".equals(sjt_status) && !"99".equals(sjt_status)
							&& StringUtils.isNotBlank(columns.getString("ORDER_NO")) && StringUtils.isNotBlank(entryStationCode)
							&& StringUtils.isNotBlank(saleDate)) {
				return true;
			} else {
				return false;
			}
		} else if ("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)) {		
			//先享后付  SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
			String seq = columns.getString("TIKCET_TRANS_SEQ");
			String cardNum = columns.getString("METRO_MEMBER_CARD_NUM");
			String transDate = columns.getString("TRANS_DATE");
			if (!StringUtils.isNotBlank(seq) || !StringUtils.isNotBlank(cardNum) || !StringUtils.isNotBlank(transDate)) {
				return false;
			}
			transDate = DateUtils.formatDate(DateUtils.parseDate(columns.getString("TRANS_DATE"),"yyyyMMdd"));
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		String pubDate = LastTimeUtils.getLastTimeDate();
		String datetime = "2018-03-07 10:00:00";
		String day = DateUtils.formatDate(DateUtils.parseDate(datetime,"yyyy-MM-dd HH:mm:ss"));
		//如果是缓存日期的数据，则发布数据，
		if(day.compareTo(pubDate) >= 0){
			//发布时间，小于缓存时间则不发布
			boolean result = LastTimeUtils.canPublishLastTime(datetime);
			System.out.println(result);
		}
		System.out.println(day + "   " + pubDate + "   |" + day.compareTo(pubDate));
	}
	
}
