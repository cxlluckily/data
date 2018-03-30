/**
 * 
 */
package com.shankephone.data.visualization.computing.user.streaming;

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
 * 当日交易量-产品（用户来源） 
 * @author yaoshijie  
 * @date 2018年2月1日  
 * @Description: TODO  
 */
public class UserSourceProcessor implements SparkStreamingProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(UserSourceProcessor.class);
	
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args){
		JavaPairRDD<String, Integer> results = rdd.filter(f->{
			return filterTicketData(f);
		}).flatMapToPair(f->{
			return mapToUseful(f);
		}).reduceByKey((a,b)->a+b);
		
		List<Tuple2<String,Integer>> records = results.collect();
		 
		 for(Tuple2<String, Integer> record : records){
			 String key = record._1;
			 String[] keys=key.split("_");
			 String cityCode=keys[0];
			 String type=keys[1];
			 String date=keys[2];
			 countTickets(type, cityCode, date, record._2);
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
	public static boolean filterTicketData(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		if(tableName == null || columns == null) {
			return false;
		}
		if (StringUtils.isNotBlank(columns.getString("CITY_CODE"))) {
			if ("SKP:ORDER_INFO".equals(tableName) ||"SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)) {
				return filterData(tableName,columns,value);
			}
		}
		return false;
	}
	public static boolean filterData(String tableName,JSONObject columns,JSONObject value){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		boolean isValid = false;
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		String cityCode=columns.getString("CITY_CODE");
		JSONObject changes=null;
		if(value.containsKey("changes")){
			changes = value.getJSONObject("changes");
		}
		String payMentType="";
		if(columns.containsKey("PAY_PAYMENT_TYPE")){
			payMentType=columns.getString("PAY_PAYMENT_TYPE");
		}
		String payState="";//tr_order_thrid --支付状态 1 待支付 2 支付成功 3 支付失败 7订单关闭(订单取消)
		String PreviousPayState = "";
		if("SKP:ORDER_INFO".equals(tableName)){
			if(columns.containsKey("PAY_STATE")){
				payState=columns.getString("PAY_STATE");
			}
			if(changes!=null && changes.containsKey("PAY_STATE")){
				PreviousPayState=changes.getString("PAY_STATE");
			}
		}
		if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){//先享后付,出自 SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
			String seq = columns.getString("TIKCET_TRANS_SEQ");
			String cardNum = columns.getString("METRO_MEMBER_CARD_NUM");
			String transDate = columns.getString("TRANS_DATE");
			if (StringUtils.isNotBlank(seq) && StringUtils.isNotBlank(cardNum) && StringUtils.isNotBlank(transDate)) {
				transDate = DateUtils.formatDate(DateUtils.parseDate(columns.getString("TRANS_DATE"),"yyyyMMdd"));
				RSet<String> set = redisson.getSet("user:source:uniq:xxhf:" + cityCode + ":" + transDate);
				return set.add(seq+"_"+cardNum);
			}
		}
		if(payMentType == null || "".equals(payMentType)||payState==null || "".equals(payState)){
			return false;
		}else if("1".equals(columns.getString("ORDER_TYPE")) && !"5190".equals(cityCode)&&StringUtils.isBlank(columns.getString("XFHX_SJT_STATUS"))){//单程票
			String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
			String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			String orderStatus = columns.getString("TICKET_ORDER_STATUS");
			String PreviousOrderStatus = "";
			if(changes!=null&&changes.containsKey("TICKET_ORDER_STATUS")){
				PreviousOrderStatus = changes.getString("TICKET_ORDER_STATUS");
			}
			if(StringUtils.isNotBlank(ticketNum)&&StringUtils.isNotBlank(time)){
				if("2".equals(payState) && "5".equals(orderStatus) && !"5".equals(PreviousOrderStatus) && !"".equals(PreviousOrderStatus)){
					isValid = true;
				}else if("5".equals(orderStatus) && "2".equals(payState) && !"2".equals(PreviousPayState) && !"".equals(PreviousPayState)){
					isValid = true;
				}else{
					isValid = false;
				}
			}
		}else if("2".equals(columns.getString("ORDER_TYPE"))){//长沙NFC充值
			String orderStatus=columns.getString("TOPUP_ORDER_STATUS");
			String PreviousOrderStatus="";
			if(changes!=null&&changes.containsKey("TOPUP_ORDER_STATUS")){
				PreviousOrderStatus = changes.getString("TOPUP_ORDER_STATUS");
			}
			if("2".equals(payState) && "5".equals(orderStatus) && !"5".equals(PreviousOrderStatus) && !"".equals(PreviousOrderStatus)){
				isValid = true;
			}else if("5".equals(orderStatus) && "2".equals(payState) && !"2".equals(PreviousPayState) && !"".equals(PreviousPayState)){
				isValid = true;
			}else{
				isValid = false;
			}
		}else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（没有站点信息）
			String ticketNum=columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
			String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			String orderStatus = columns.getString("TICKET_ORDER_STATUS");
			String PreviousOrderStatus = "";
			if(changes!=null&&changes.containsKey("TICKET_ORDER_STATUS")){
				PreviousOrderStatus = changes.getString("TICKET_ORDER_STATUS");
			}
			if(StringUtils.isNotBlank(ticketNum)&&StringUtils.isNotBlank(time)){
				if("2".equals(payState) && "5".equals(orderStatus) && !"5".equals(PreviousOrderStatus) && !"".equals(PreviousOrderStatus)){
					isValid = true;
				}else if("5".equals(orderStatus) && "2".equals(payState) && !"2".equals(PreviousPayState) && !"".equals(PreviousPayState)){
					isValid = true;
				}else{
					isValid = false;
				}
			}
		}else if(!"03".equals(columns.getString("XFHX_SJT_STATUS")) && !"99".equals(columns.getString("XFHX_SJT_STATUS"))){//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
			String saleDate = columns.getString("XFHX_SJT_SALE_DATE");
			if(StringUtils.isNotBlank(saleDate) && "2".equals(payState)){
				saleDate = saleDate.split(" ")[0];
				RSet<String> set = redisson.getSet("user:source:uniq:xfhx:" + cityCode + ":" + saleDate);
				isValid = set.add(columns.getString("ORDER_NO"));
			}
		}else if("5".equals(columns.getString("ORDER_TYPE"))){//蜂格咖啡
			String time = columns.getString("COFFEE_UPDATE_DATE");
			String orderStatus = columns.getString("COFFEE_ORDER_STATE");
			String PreviousOrderStatus = "";
			if(changes!=null&&changes.containsKey("COFFEE_ORDER_STATE")){
				PreviousOrderStatus = changes.getString("COFFEE_ORDER_STATE");
			}
			if(StringUtils.isNotBlank(time)){
				if("2".equals(payState) && "4".equals(orderStatus) && !"4".equals(PreviousOrderStatus) && !"".equals(PreviousOrderStatus)){
					isValid = true;
				}else if("4".equals(orderStatus) && "2".equals(payState) && !"2".equals(PreviousPayState) && !"".equals(PreviousPayState)){
					isValid = true;
				}else{
					isValid = false;
				}
			}
		}
		return isValid;
	}
	
	
	/**
	 * 组装
	* @Title: mapToUseful 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param value
	* @param @return    参数  
	* @return Iterator<Map<String,Integer>>    返回类型  
	* @throws
	 */
	public static Iterator<Tuple2<String,Integer>> mapToUseful(JSONObject value){
		List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String,Integer>>();
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		Integer num = 0;
		String type = getType(columns,tableName);
		Map<String , Object> m = getDate(tableName,columns,value);
		String date=(String) m.get("date");
		num=(Integer) m.get("ticketNum");
		if(!"".equals(date)&&!"".equals(type)&& type!=null && date!=null){
			String cityKeyStr=cityCode+"_"+type+"_"+date;
			String nationKeyStr="0000_"+type+"_"+date;
			result.add(new Tuple2<String, Integer>(cityKeyStr, num));
			result.add(new Tuple2<String, Integer>(nationKeyStr, num));
		}
		return result.iterator();
	}
	public static String getType(JSONObject columns,String tableName){
		String type=null;
		String payType ="";
		if("SKP:ORDER_INFO".equals(tableName)){
			payType = columns.getString("PAY_PAYMENT_TYPE");
			if(Constants.SKF_PAY_TYPE.indexOf(payType)>=0){//闪客蜂
				type = Constants.PAY_TYPE_SKF;
			}else if(Constants.ZFB_PAY_TYPE.indexOf(payType)>=0){//支付宝城市服务
				type = Constants.PAY_TYPE_ZFB;
			}else if(Constants.YGPJ_PAY_TYPE.indexOf(payType)>=0){//云购票机
				type = Constants.PAY_TYPE_YGPJ;
			}else if(Constants.WX_PAY_TYPE.indexOf(payType)>=0){//微信小程序
				type = Constants.PAY_TYPE_WX;
			}else if(Constants.HB_PAY_TYPE.indexOf(payType)>=0){//和包app
				type = Constants.PAY_TYPE_HB;
			}else{//其他
				type = Constants.PAY_TYPE_QT;
			}
		}else if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){
			payType=columns.getString("PROVIDER_ID");
			if("01".equals(payType)){//渠道编码  01：广州地铁 02：闪客蜂 11：支付宝 12：微信
				type = Constants.PAY_TYPE_GZDT;
			}else if("02".equals(payType)){
				type = Constants.PAY_TYPE_SKF;
			}else if("11".equals(payType)){
				type = Constants.PAY_TYPE_ZFB;
			}else if("12".equals(payType)){
				type = Constants.PAY_TYPE_WX;
			}else{//其他
				type = Constants.PAY_TYPE_QT;
			}
		}
		
		return type;
	}
	public static Map<String , Object> getDate(String tableName,JSONObject columns,JSONObject value){
		Map<String , Object> map = new HashMap<String, Object>();
		String date="";
		Integer ticketNum=0;
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		if ("SKP:ORDER_INFO".equals(tableName) ) {
			if ("1".equals(columns.getString("ORDER_TYPE"))&&StringUtils.isBlank(columns.getString("XFHX_SJT_STATUS"))) { 				//单程票
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			} else if ("2".equals(columns.getString("ORDER_TYPE"))) {	//  长沙充值
				ticketNum = 1;
				date = columns.getString("TOPUP_TOPUP_DATE").split(" ")[0];
			}else if("5".equals(columns.getString("ORDER_TYPE"))){
				ticketNum = 1;
				date = columns.getString("COFFEE_UPDATE_DATE").split(" ")[0];
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {//先付后享  珠海有轨（没有站点信息）
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			}else{//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
				ticketNum = 1;
				date = columns.getString("XFHX_SJT_SALE_DATE").split(" ")[0];
			}
		}else if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){//先享后付,出自 SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
			ticketNum = 1;
			date = DateUtils.formatDate(DateUtils.parseDate(columns.getString("TRANS_DATE"),"yyyyMMdd"));
		}
		map.put("date", date);
		map.put("ticketNum", ticketNum);
		return map;
	}
	
	
	/**
	 * 来源统计
	* @Title: countTickets 
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
	public static void countTickets(String type, String cityCode, String date, Integer ticketNum){
		 RedissonClient redisson =  RedisUtils.getRedissonClient();
		JSONObject json = new JSONObject();
		String trandeKey = "user:source:vol:" + cityCode;
		RMap<String, Map<String, Integer>> tradeMap = redisson.getMap(trandeKey);
		Map<String, Integer> tradeData = tradeMap.getOrDefault(date, new HashMap<>());
		int lastOrderNum = tradeData.get(type) == null ? 0 : tradeData.get(type);
		tradeData.put(type, lastOrderNum + ticketNum);
		tradeMap.put(date, tradeData);
		boolean result = LastTimeUtils.canPublishData(date);
		Map<String, Integer> currentData = tradeMap.get(DateUtils.getCurrentDate());
		if (currentData != null&&result) {
			for(String key : currentData.keySet()){
				json.put(key, currentData.get(key));
			}
			redisson.getTopic(trandeKey).publish(json.toString());				//推送交易数据
		}
	}
}
