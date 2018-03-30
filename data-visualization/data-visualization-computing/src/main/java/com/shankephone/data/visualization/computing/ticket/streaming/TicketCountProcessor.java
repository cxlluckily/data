package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;


/**
 * 售票量 与 热门站点 统计
 * @author 森
 * @version 2017年9月15日 下午2:02:35
 */
public class TicketCountProcessor implements SparkStreamingProcessor {
	private final static Logger logger = LoggerFactory.getLogger(TicketCountProcessor.class);
	
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args){
		//计算
		JavaPairRDD<String, Integer> results = rdd.filter(f->{
			return filterTicketData(f);
		}).mapToPair(f->{
			return mapToUseful(f);
		}).reduceByKey((a,b)->a+b).cache();
		//输出
		 List<Tuple2<String,Integer>> records = results.collect();
		 //全国的map: Map<天,Map<类型,数量>>
		 Map<String,Map<String,Integer>> nationMap = new HashMap<String,Map<String,Integer>>();
		 //城市-天-类型-数量
		 Map<String,Map<String,Map<String,Integer>>> cityMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		 
		 //遍历结果集
		 for(Tuple2<String,Integer> tuple : records){
			 String key = tuple._1;
			 Integer count = tuple._2;
			 String [] keys = key.split("_");
			 String cityCode = keys[0];
			 String day = keys[1];
			 String type = keys[2];
			 //天汇总
			 Map<String,Map<String,Integer>> dayMap = cityMap.get(cityCode);
			 if(dayMap == null){
				 dayMap = new HashMap<String,Map<String,Integer>>();
			 }
			 //类型Map
			 Map<String,Integer> typesMap = dayMap.get(day);
			 if(typesMap == null){
				 typesMap = new HashMap<String,Integer>();
			 }
			 //类型数量
			 Integer typeCount = typesMap.get(type);
			 if(typeCount == null){
				 typeCount = 0;
			 }
			 typeCount += count;
			 //存入具体类型
			 typesMap.put(type, typeCount);
			 dayMap.put(day, typesMap);
			 cityMap.put(cityCode, dayMap);
			 
			 //全国数据汇总
			 Map<String,Integer> totalTypesMap = nationMap.get(day);
			 if(totalTypesMap == null){
				 totalTypesMap = new HashMap<String,Integer>();
			 }
			 //全国各类型汇总
			 Integer totalTypeCount = totalTypesMap.get(type);
			 if(totalTypeCount == null){
				 totalTypeCount = 0;
			 }
			 totalTypeCount += count;
			 totalTypesMap.put(type, totalTypeCount);
			 Integer cityCount = totalTypesMap.get(cityCode);
			 if(cityCount == null){
				 cityCount = 0;
			 }
			 cityCount += count;
			 totalTypesMap.put(cityCode, cityCount);
			 
			 //全国票量汇总
			 Integer totalCount = totalTypesMap.get("all");
			 if(totalCount == null){
				 totalCount = 0;
			 }
			 totalCount += count;
			 totalTypesMap.put("all", totalCount);
			 nationMap.put(day, totalTypesMap);
		 }
		 
		 //汇总每个城市的所有类型
		 for(String cityCode : cityMap.keySet()){
			 Map<String,Integer> typesMap =  new HashMap<String,Integer>();
			 Map<String,Map<String,Integer>> dayMap = cityMap.get(cityCode);
			 for(String day :dayMap.keySet()){
				 typesMap = dayMap.get(day);
				 Integer all = 0;
				 for(String type : typesMap.keySet()){
					 all += typesMap.get(type);
				 }
				 typesMap.put("all", all);
				 dayMap.put(day, typesMap);
			 }
		 }
		 
		 //全国-天-类型-数量
		 Map<String,Map<String,Map<String,Integer>>> totalMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		 totalMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		 totalMap.put(Constants.NATION_CODE, nationMap);
		 processData(cityMap);
		 processData(totalMap);
		 
	}

	/**
	 * 发布-缓存数据
	 * @param cityMap
	 */
	private void processData(Map<String, Map<String, Map<String, Integer>>> cityMap) {
		for(String cityCode : cityMap.keySet()){
			Map<String,Map<String,Integer>> dayMap = cityMap.get(cityCode);
			for(String day : dayMap.keySet()){
				Map<String,Integer> typesMap = dayMap.get(day);
				if(typesMap == null){
					typesMap = new HashMap<String,Integer>();
				}
				RMap<String,Map<String,Integer>> cacheMap = RedisUtils.getRedissonClient().getMap("trade:vol:" + cityCode);
				//获取redis缓存的数据日期
				String last_date = LastTimeUtils.getLastTimeDate();
				boolean isPublicDayTickets=false;//标记是否推送每日售票量
				Map<String,Integer> map = cacheMap.get(last_date);//根据最新更新时间判断日交易统计中是否有该时间的数据，如果没有则发布最新每日售票量数据
				if(map==null){
					isPublicDayTickets=true;
				}
				Map<String,Integer> cacheTypesMap = cacheMap.get(day);
				if(cacheTypesMap == null){
					cacheTypesMap = new HashMap<String,Integer>();
				}
				for(String type : typesMap.keySet()){
					//按城市放入缓存
					Integer cacheCount = cacheTypesMap.get(type);
					if(cacheCount == null){
						cacheCount = 0;
					}
					cacheCount += typesMap.get(type);
					cacheTypesMap.put(type, cacheCount);
				}
				cacheMap.put(day, cacheTypesMap);
				boolean result = LastTimeUtils.canPublishData(day);
				if(result){
					JSONObject json = new JSONObject();
					json.putAll(cacheTypesMap);
					RedisUtils.getRedissonClient().getTopic("trade:vol:" + cityCode).publish(json.toJSONString());
				}
				
				
				//判断是否更新推送每日售票量
				if(isPublicDayTickets){
					processTicketsBydays(cacheMap,last_date,cityCode);
				}
			}
		}
	}
	
	/**
	 * 缓存每日售票量，只缓存当日售票量
	 * @param cityCode
	 * @param date
	 * @param redisson
	 * @param ticket_num
	 */
	public static void processTicketsBydays(RMap<String,Map<String,Integer>> cacheMap,String last_date,String cityCode){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(DateUtils.parseDate(last_date));
		
		//计算30天中第一天的日期
		calendar.add(Calendar.DATE, -30);
		String before = DateUtils.formatDate(calendar.getTime());
		
		/**  发布数据  **/
		//publish 城市代码_tickets_days
		JSONObject day_tickets = new JSONObject();
		List<String> time = new ArrayList<String>();
		List<Integer> tickets = new ArrayList<Integer>();
		int i = 0;
		while (i<30) {
			if(cacheMap.get(before)!=null && cacheMap.get(before).containsKey("all")){
				tickets.add(cacheMap.get(before).get("all"));	
			}else{
				tickets.add(0);
			}
			time.add(before);
			calendar.add(Calendar.DATE, 1);
			before = DateUtils.formatDate(calendar.getTime());
			i++;
		}
		
		day_tickets.put("TIME", time);
		day_tickets.put("TICKETS", tickets);
		RedisUtils.getRedissonClient().getTopic("day:tickets:"+cityCode)
		   .publish(day_tickets.toJSONString());
		logger.info("【day:tickets:"+cityCode+"】发布成功! "+day_tickets.toString());
	}
	
	/**
	 * 过滤有效单程票/先付后享/先享后付/咖啡/长沙圈存数据/用户
	 * 并推送票数据的站点信息
	 * @author senze
	 * @date 2018年1月25日 下午5:05:45
	 * @param value
	 * @param timestamp
	 * @return
	 */
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
			//过滤先付后享-非珠海（乘车码）、先享后付
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
			if ("1".equals(columns.getString("ORDER_TYPE"))&&!"5190".equals(cityCode)&&StringUtils.isBlank(columns.getString("XFHX_SJT_STATUS"))) { 				
				//单程票
				String entryStationCode = columns.getString("TICKET_PICKUP_STATION_CODE");
				String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
				String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
				statusFlag = "5".equals(columns.getString("TICKET_ORDER_STATUS"));
				preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
				ticketFlag = StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(ticketNum);
				dateFlag = StringUtils.isNotBlank(time);
			} else if ("2".equals(columns.getString("ORDER_TYPE"))) {	
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
	//过滤先付后享-非珠海（乘车码）、先享后付
	public static boolean filterPartTwo(String tableName,JSONObject columns,JSONObject value,String cityCode,RedissonClient redisson){
		if ("SKP:SJT_QR_CODE".equals(tableName)) { 	
			//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
			String saleDate = columns.getString("SJT_SALE_DATE");
			String sjt_status = columns.getString("SJT_STATUS");
			if (sjt_status != null && !"03".equals(sjt_status) && !"99".equals(sjt_status)
							&& StringUtils.isNotBlank(saleDate)) {
				saleDate = saleDate.split(" ")[0];
				RSet<String> set = redisson.getSet("order:uniq:" + "xfhx:" + cityCode + ":" + saleDate);
				if (set.add(columns.getString("ORDER_NO"))) {
					return true;
				}
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
			RSet<String> set = redisson.getSet("order:uniq:" + "xxhf:" + cityCode + ":" + transDate);
			return set.add(seq+"_"+cardNum);
		}
		return false;
	}
	/**
	 * 保留统计有效信息
	 * @author senze
	 * @date 2018年1月25日 下午5:01:11
	 * @param value
	 * @return
	 */
	public static Tuple2<String,Integer> mapToUseful(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		String type = null;
		Integer ticketNum = 0;
		String date = null;
		
		if ("SKP:ORDER_INFO".equals(tableName) ) {
			String productCode = columns.getString("ORDER_PRODUCT_CODE");
			String productCodePre = productCode.split("_")[0];
			if ("DCP".equals(productCodePre) || "TVIP".equals(productCodePre) || "GZH".equals(productCodePre)) { 				//单程票
				type = Constants.ORDER_TYPE_DCP;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
				
			} else if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	//  长沙充值
				type = Constants.ORDER_TYPE_NFC;
				ticketNum = 1;
				date = columns.getString("TOPUP_TOPUP_DATE").split(" ")[0];
				
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（无站点信息）
				type = Constants.ORDER_TYPE_XFHX;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			}
			
		} else if ("COFFEE:T_TASTE_ORDER".equals(tableName)) {			//咖啡
			type = Constants.ORDER_TYPE_COFFEE;
			ticketNum = 1;
			date = columns.getString("UPDATE_DATE").split(" ")[0];
			
		} else if ("SKP:SJT_QR_CODE".equals(tableName)) { 	//先付后享 非珠海（乘车码）
			type = Constants.ORDER_TYPE_XFHX;
			ticketNum = 1;
			date = columns.getString("SJT_SALE_DATE").split(" ")[0];
			
		} else if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){			//先享后付
			type = Constants.ORDER_TYPE_XXHF;
			ticketNum = 1;
			date = DateUtils.formatDate(DateUtils.parseDate(columns.getString("TRANS_DATE"),"yyyyMMdd"));
		}
		String key = cityCode + "_" + date + "_" + type; 
		Map<String, Integer>map = new HashMap<String,Integer>();
		map.put(key, ticketNum);
		Tuple2<String,Integer> tuple1 = new Tuple2<String,Integer>(key, ticketNum);
		return tuple1;
		
	}
	
}
