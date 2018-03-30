package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
 * 当日支付方式
 * @author fengql
 * @version 2017年9月20日 上午9:40:41
 */
public class PayTypeProcessor implements SparkStreamingProcessor{
	
	private final static Logger logger = LoggerFactory.getLogger(PayTypeProcessor.class);
	
	/**
	 * 集成任务接口实现
	 * @param rdd
	 * @param args
	 */
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args) {
		JavaPairRDD<String, Integer> pairRdd = rdd.filter(f->{
			return filterTicketDataPayType(f);
		}).flatMapToPair(f->{
			return mapToUsefulForPayType(f);
		}).reduceByKey((a,b)->a+b);
		List<Tuple2<String,Integer>> list = pairRdd.collect();
		for(Tuple2<String, Integer> record : list){
			 try {
				String key = record._1;
				 String[] keys=key.split("_");
				 String cityCode=keys[0];
				 String type=keys[1];
				 String date=keys[2];
				 if(cityCode == null || "".equals(cityCode)){
					 continue;
				 }
				 if(date == null || "".equals(date)){
					 continue;
				 }
				 countTicketsPayType(type, cityCode, date, record._2);
			} catch (Exception e) {
				e.printStackTrace();
				logger.warn(record._1 + "," + record._2);
				throw new RuntimeException(e);
			}
		 }
	}	
	
	/**
	 * 
	* @Title: filterTicketData 
	* @author yaoshijie  
	* @Description: 过滤有效的交易量：闪客蜂、小程序、城市服务
	* @param @param value
	* @param @param timestamp
	* @param @return    参数  
	* @return boolean    返回类型  
	* @throws
	 */
	public static boolean filterTicketDataPayType(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		if(tableName == null || columns == null) {
			return false;
		}
		if (StringUtils.isNotBlank(columns.getString("CITY_CODE")) 
				&& columns.containsKey("PAY_PAY_TIME") 
				&& columns.containsKey("PAY_PAYMENT_TYPE")
				&& columns.getString("PAY_PAYMENT_TYPE") != null 
				&& !"".equals(columns.getString("PAY_PAYMENT_TYPE"))) {
			if ("SKP:ORDER_INFO".equals(tableName)) {
				return filterDataPayType(tableName,columns,value);
			}
		}
		return false;
	}
	public static boolean filterDataPayType(String tableName,JSONObject columns,JSONObject value){
		String payState = "";
		String prePayState = "";
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		if(columns.containsKey("PAY_STATE")){
			payState = columns.getString("PAY_STATE");
		}
		JSONObject changes = value.getJSONObject("changes");
		String cityCode = columns.getString("CITY_CODE");
		if(changes != null && changes.containsKey("PAY_STATE")){
			prePayState = changes.getString("PAY_STATE");
		}
		if("1".equals(columns.getString("ORDER_TYPE")) 
		  	&& StringUtils.isBlank(columns.getString("XFHX_SJT_STATUS"))
		    &&!"5190".equals(cityCode)){//单程票
			//单程票
			String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
			String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			String status = columns.getString("TICKET_ORDER_STATUS");
			String preStatus = "";
			if(changes != null && changes.containsKey("TICKET_ORDER_STATUS")){
				preStatus = changes.getString("TICKET_ORDER_STATUS");
			}
			if(StringUtils.isNotBlank(ticketNum)&&StringUtils.isNotBlank(time)){
				return (!"5".equals(preStatus) && "5".equals(status) && "2".equals(payState))
						|| (!"2".equals(prePayState) && "2".equals(payState) && "5".equals(status));
			}
			return false;
		}
		if("2".equals(columns.getString("ORDER_TYPE"))){//长沙NFC充值
			//长沙充值
			String status = columns.getString("TOPUP_ORDER_STATUS");
			String preStatus = "";
			if(changes != null && changes.containsKey("TOPUP_ORDER_STATUS")){
				preStatus = changes.getString("TOPUP_ORDER_STATUS");
			}
			return (!"5".equals(preStatus) && "5".equals(status) && "2".equals(payState))
					|| (!"2".equals(prePayState) && "2".equals(payState) && "5".equals(status));
		}
		
		if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		
			//珠海有轨（没有站点信息）
			String ticketNum=columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
			String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			String status = columns.getString("TICKET_ORDER_STATUS");
			String preStatus = "";
			if(changes != null && changes.containsKey("TICKET_ORDER_STATUS")){
				preStatus = changes.getString("TICKET_ORDER_STATUS");
			}
			if(StringUtils.isNotBlank(ticketNum)&&StringUtils.isNotBlank(time)){
				return (!"5".equals(preStatus) && "5".equals(status) && "2".equals(payState))
						|| (!"2".equals(prePayState) && "2".equals(payState) && "5".equals(status));
			}
			return false;
		}
		if("3".equals(columns.getString("ORDER_TYPE")) 
				&& StringUtils.isNotBlank(columns.getString("XXHF_DEBIT_REQUEST_RESULT"))
				&& StringUtils.isNotBlank(columns.getString("XXHF_ORDER_DATE"))
				&& StringUtils.isNotBlank(cityCode) 
				&& StringUtils.isNotBlank(columns.getString("PAY_PAYMENT_TYPE"))
				){
			//先享后付
			String status = columns.getString("XXHF_DEBIT_REQUEST_RESULT");
			String preStatus = null;
			if(changes != null && changes.containsKey("XXHF_DEBIT_REQUEST_RESULT")){
				preStatus = changes.getString("XXHF_DEBIT_REQUEST_RESULT");
			}
			return (!"0".equals(preStatus) && "0".equals(status) && "2".equals(payState))
					|| (changes == null && "0".equals(status) && "2".equals(payState));
		}
		if(null != columns.getString("XFHX_SJT_STATUS") 
				&& columns.containsKey("XFHX_SJT_STATUS") 
		   		&& !"".equals(columns.getString("XFHX_SJT_STATUS")) 
				&& !"03".equals(columns.getString("XFHX_SJT_STATUS")) 
				&& !"99".equals(columns.getString("XFHX_SJT_STATUS"))
				&& "2".equals(payState) 
				&& StringUtils.isNotBlank(columns.getString("PAY_PAYMENT_TYPE"))
				&& StringUtils.isNotBlank(columns.getString("XFHX_SJT_SALE_DATE"))
				&& StringUtils.isNotBlank(cityCode)
			){
			//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
			String saleDate = columns.getString("XFHX_SJT_SALE_DATE").split(" ")[0]; 
			String key = "pay:type:uniq:xfhx:" + cityCode + ":" + saleDate;
			RedissonClient redisson =  RedisUtils.getRedissonClient();
			RSet<String> rset = redisson.getSet(key);
			return rset.add(columns.getString("ORDER_NO"));
		}
		
		if("5".equals(columns.getString("ORDER_TYPE"))){
			//蜂格咖啡
			String time = columns.getString("COFFEE_UPDATE_DATE");
			String status = columns.getString("COFFEE_ORDER_STATE");
			String preStatus = "";
			if(changes != null && changes.containsKey("COFFEE_ORDER_STATE")){
				preStatus = changes.getString("COFFEE_ORDER_STATE");
			}
			if(StringUtils.isNotBlank(time)){
				return (!"4".equals(preStatus) && "4".equals(status) && "2".equals(payState))
						|| (!"2".equals(prePayState) && "2".equals(payState) && "4".equals(status));
			}
			return false;
		}
		return false;
	}
	/**
	 * 支付方式数据组装
	* @Title: mapToUsefulForPayMent 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param value
	* @param @return    参数  
	* @return Iterator<Tuple2<Map<String,String>,Integer>>    返回类型  
	* @throws
	 */
	public static Iterator<Tuple2<String,Integer>> mapToUsefulForPayType(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		String pay_payment_type = columns.getString("PAY_PAYMENT_TYPE");
		Integer num = 0;
		String payment_code = "";
		if("0".equals(pay_payment_type) || "2".equals(pay_payment_type) 
				|| "6".equals(pay_payment_type) || "10".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_ZIFB;
		}else if("3".equals(pay_payment_type) || "4".equals(pay_payment_type) 
				|| "7".equals(pay_payment_type) || "12".equals(pay_payment_type)
				|| "13".equals(pay_payment_type) || "14".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_WEIX;
		}else if("1".equals(pay_payment_type) || "9".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_ZHONGYD;
		}else if("5".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_YIZF;
		}else if("8".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_SHOUXYZF;
		}else if("11".equals(pay_payment_type) || "YLSF".equals(pay_payment_type)){
			payment_code = Constants.PAYMENT_TYPE_YINL;
		}else{
			payment_code = Constants.PAYMENT_TYPE_OTHER;
		}
		payment_code = payment_code != null ? payment_code.toLowerCase() : payment_code;
		Map<String , Object> m = getDatePayType(tableName,columns,value);
		String date=(String) m.get("date");
		num=(Integer) m.get("ticketNum");
		String cityKeyStr=cityCode+"_"+payment_code+"_"+date;
		String nationKeyStr="0000_"+payment_code+"_"+date;
		List<Tuple2<String, Integer>> result = new ArrayList<>();
		result.add(new Tuple2<String, Integer>(cityKeyStr, num));
		result.add(new Tuple2<String, Integer>(nationKeyStr, num));
		return result.iterator();
	}
	public static Map<String , Object> getDatePayType(String tableName,JSONObject columns,JSONObject value){
		Map<String , Object> map = new HashMap<String, Object>();
		String date="";
		Integer ticketNum=0;
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		if ("SKP:ORDER_INFO".equals(tableName) ) {
			String cityCode = columns.getString("CITY_CODE");
			if ("1".equals(columns.getString("ORDER_TYPE")) 
					&& StringUtils.isBlank(columns.getString("XFHX_SJT_STATUS"))
					&& !"5190".equals(cityCode)) { 				//单程票
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			} else if ("2".equals(columns.getString("ORDER_TYPE"))) {	//  长沙充值
				ticketNum = 1;
				date = columns.getString("TOPUP_TOPUP_DATE").split(" ")[0];
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//先付后享   -- 珠海有轨（无站点信息）
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			} else if("5".equals(columns.getString("ORDER_TYPE"))){
				ticketNum = 1;
				date = columns.getString("COFFEE_UPDATE_DATE").split(" ")[0];
			}else if("3".equals(columns.getString("ORDER_TYPE")) 
					&& StringUtils.isNotBlank(columns.getString("XXHF_DEBIT_REQUEST_RESULT"))
					&& StringUtils.isNotBlank(columns.getString("XXHF_ORDER_DATE"))
					&& StringUtils.isNotBlank(cityCode) 
					&& StringUtils.isNotBlank(columns.getString("PAY_PAYMENT_TYPE"))
					&& StringUtils.isNotBlank(columns.getString("XXHF_METRO_MEMBER_CARD_NUM"))
					&& StringUtils.isNotBlank(columns.getString("XXHF_TIKCET_TRANS_SEQ"))
					&& "0".equals(columns.getString("XXHF_DEBIT_REQUEST_RESULT"))
					&& "2".equals(columns.getString("PAY_STATE"))){//先享后付 （没有上下站点信息字段  record里有   表连接需求）
				ticketNum = 1;
				date = DateUtils.formatDate(DateUtils.parseDate(columns.getString("XXHF_ORDER_DATE"),"yyyy-MM-dd"));
				if("".equals(date)){
					logger.error(columns.toJSONString()); 
				}
			}else if(null != columns.getString("XFHX_SJT_STATUS") 
					&& columns.containsKey("XFHX_SJT_STATUS") 
			   		&& !"".equals(columns.getString("XFHX_SJT_STATUS")) 
					&& !"03".equals(columns.getString("XFHX_SJT_STATUS")) 
					&& !"99".equals(columns.getString("XFHX_SJT_STATUS"))
					&& "2".equals(columns.getString("PAY_STATE")) 
					&& StringUtils.isNotBlank(columns.getString("PAY_PAYMENT_TYPE"))
					&& StringUtils.isNotBlank(columns.getString("XFHX_SJT_SALE_DATE"))
					&& StringUtils.isNotBlank(cityCode)
			){//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
				date = columns.getString("XFHX_SJT_SALE_DATE");
				if(date != null && StringUtils.isNotBlank(date)){
					ticketNum = 1;
					date = columns.getString("XFHX_SJT_SALE_DATE").split(" ")[0];
				}
			}
			if(StringUtils.isBlank(date)){
				System.out.println(value.toJSONString()); 
			}
		}
		map.put("date", date);
		map.put("ticketNum", ticketNum);
		return map;
	}
	/**
	 * 支付方式数据统计
	* @Title: countTicketsPayMent 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param type
	* @param @param cityCode
	* @param @param date
	* @param @param ticketNum
	* @param @param redisson    参数  
	* @return void    返回类型  
	* @throws
	 */
	public static void countTicketsPayType(String type, String cityCode, String date, Integer ticketNum){
		RedissonClient redisson =  RedisUtils.getRedissonClient();
		JSONObject json = new JSONObject();
		String trandeKey = Constants.REDIS_NAMESPACE_TICKET_PAYMENT + cityCode;
		RMap<String, Map<String, Integer>> tradeMap = redisson.getMap(trandeKey);
		Map<String, Integer> tradeData = tradeMap.getOrDefault(date, new HashMap<>());
		int lastOrderNum = tradeData.get(type) == null ? 0 : tradeData.get(type);
		tradeData.put(type, lastOrderNum + ticketNum);
		try {
			tradeMap.put(date, tradeData);
		} catch (Exception e) {
			logger.error("tradeMap:  " + tradeMap.toString());
			logger.error("date:  " + date);
			logger.error("tradeData:  " + tradeData.toString());
			throw new RuntimeException(e);
		}
		//获取当前的最新发布数据时间
//		String pubDay = IAnalyseHandler.getPublishDate();
		Map<String, Integer> currentData = tradeMap.get(DateUtils.getCurrentDate());
		if (currentData != null) {
			for(String key : currentData.keySet()){
				json.put(key, currentData.get(key));
			}
			boolean res = LastTimeUtils.canPublishData(date);
			//发布等于缓存日期的数据
			if(res){
				//推送交易数据
				redisson.getTopic(trandeKey).publish(json.toString());	
				//logger.warn("发布成功：" + json.toJSONString());
			}
						
		}
	}
	
	public static void main(String[] args) {
		String json = "{\"columns\":{\"PAY_PAYMENT_TYPE\":\"10\",\"XXHF_DEBIT_REQUEST_ORDER_NO\":\"05201803200001403043\",\"XXHF_CHARGE_TYPE\":\"1\",\"XXHF_ENTRY_STATION_CODE\":\"0701\",\"PAY_REFUND_TIME\":\"\",\"PAY_PARTNER_NO\":\"510000003\",\"XXHF_EXIT_LINE_CODE\":\"06\",\"XXHF_RESERVE\":\"\",\"XXHF_ENTY_PAY_TRANS_RECORD\":\"\",\"PAY_STATE\":\"2\",\"XXHF_EXIT_DEVICE_TYPE\":\"04\",\"XXHF_EXIT_DATE\":\"2018-03-20 00:01:39\",\"PAY_ORDER_ID\":\"A1594333908578844671\",\"XXHF_ENTRY_LINE_CODE\":\"07\",\"PAY_EXT_ORDER_ID\":\"05201803200001403043\",\"XXHF_EXIT_STATION_CODE\":\"0632\",\"XXHF_ID\":\"883916\",\"XXHF_REMARK\":\"\",\"XXHF_EXIT_PAY_TRANS_RECORD\":\"\",\"PAY_MODIFY_TIME\":\"2018-03-20 00:01:41\",\"XXHF_TIMESTAMP\":\"1521475301000\",\"PAY_SOURCE\":\"5\",\"CITY_CODE\":\"4401\",\"XXHF_REG_DATE\":\"2018-03-20 00:01:40\",\"XXHF_ENTRY_DATE\":\"2018-03-19 22:29:21\",\"XXHF_CITY_CODE\":\"4401\",\"PAY_CASH_AMOUNT\":\"9.0\",\"XXHF_ORDER_DATE\":\"2018-03-20 00:01:40\",\"PAY_COUPON_AMOUNT\":\"0.0\",\"PAY_REFUND_STATE\":\"0\",\"PAY_ENABLED\":\"1\",\"XXHF_DEBIT_REQUEST_RESULT\":\"0\",\"XXHF_EXIT_DEVICE_CODE\":\"0632045023\",\"XXHF_ENTRY_DEVICE_TYPE\":\"04\",\"XXHF_EXIT_DEBIT_AMOUNT\":\"900\",\"XXHF_ORDER_EXP_TYPE\":\"0\",\"XXHF_DEBIT_REQUEST_RESULT_DESC\":\"扣款成功\",\"ORDER_NO\":\"05201803200001403043\",\"PAY_TIMESTAMP\":\"1521475301000\",\"XXHF_PAY_DATE\":\"2018-03-20 00:01:41\",\"XXHF_ORDER_DESC\":\"\",\"XXHF_TOTAL_AMOUT\":\"900\",\"PAY_TOTAL_AMOUNT\":\"9.0\",\"XXHF_PANCHAN_TRADE_NO\":\"A1594333908578844671\",\"XXHF_PAY_TRADE_NO\":\"2018032021001004130542926709\",\"PAY_REFUND_AMOUNT\":\"0.0\",\"XXHF_ENTRY_DEVICE_CODE\":\"0701045028\",\"ORDER_TYPE\":\"3\",\"PAY_PAY_TIME\":\"2018-03-20 00:01:41\",\"PAY_PAY_ACCOUNT\":\"2088602155940138\",\"T_LAST_TIMESTAMP\":\"1521475302016\",\"XXHF_METRO_MEMBER_CARD_NUM\":\"0180802007218104\",\"XXHF_TIKCET_TRANS_SEQ\":\"3\",\"XXHF_DEBIT_AMOUT\":\"900\",\"XXHF_PROVIDER_ID\":\"01\",\"PAY_CREATE_TIME\":\"2018-03-20 00:01:40\"},\"changes\":{\"XXHF_PAY_DATE\":\"\",\"XXHF_DEBIT_REQUEST_RESULT_DESC\":\"\",\"T_LAST_TIMESTAMP\":\"1521475302010\",\"XXHF_PAY_TRADE_NO\":\"\",\"XXHF_DEBIT_REQUEST_RESULT\":\"\",\"XXHF_PANCHAN_TRADE_NO\":\"\"},\"rowkey\":\"043d_4401_05201803200001403043\",\"schemaName\":\"XXHF4401\",\"tableName\":\"SKP:ORDER_INFO\"}";
		JSONObject jb = JSONObject.parseObject(json);
		getDatePayType("SKP:ORDER_INFO",jb.getJSONObject("columns"),null);
	}
}
