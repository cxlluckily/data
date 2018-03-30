package com.shankephone.data.visualization.computing.ticket.offline;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

/**
 * 离线计算今日用户数
 * @author 森
 * @version 2017年9月20日 下午3:52:46
 */
public class TicketCountOfflineProcessor implements Executable {

	private final static Logger logger = LoggerFactory.getLogger(TicketCountOfflineProcessor.class);
	
	public static void main(String[] args) {
		TicketCountOfflineProcessor analyse = new TicketCountOfflineProcessor();
		Map<String, Object> argsMap = new HashMap<String, Object>();
		argsMap.put("startTime", "2018-02-08 00:00:00");
		argsMap.put("endTime", "2018-02-08 20:00:00");
		argsMap.put("lastTimestamp", "2018-02-08 20:00:00");
		analyse.execute(argsMap);
	}
	
	@Override
	public void execute(Map<String, Object> argsMap) {
		analyseUserNum("ticketCount", argsMap);
	}

	/**
	 * 离线计算全国、城市的当天售票量、当日用户数和当日交易量,计算的都是当日的数据
	 * @param sqlFile
	 * @param argsMap
	 */
	private void analyseUserNum(String sqlFile, Map<String, Object> argsMap) {
		logger.info("=========================start==========================");
		String startTime=DateUtils.formatDate(new Date());//开始时间
		//离线数据处理的截止时间点
		String startTimeStr = (String) argsMap.get("startTime");
		if(startTimeStr!=null && !"".equals(startTimeStr)){
			startTime=DateUtils.formatDate(DateUtils.parseDate(startTimeStr));
		}
		String endTime = (String) argsMap.get("endTime");
		endTime=DateUtils.formatDate(DateUtils.parseDate(endTime));
		
		String lastTimestamp = (String) argsMap.get("lastTimestamp");
		Date lastTime=DateUtils.parseDateTime(lastTimestamp);
		lastTimestamp=String.valueOf(lastTime.getTime());
		logger.info("数据处理中......");
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("startTime", startTime);
		if(endTime!=null){
			params.put("endTime", endTime);
		}
		params.put("lastTimestamp", lastTimestamp );
		
		
		SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
		Dataset<Row> results = builder.executeSQL(sqlFile, params);
		
		List<Row> list = results.collectAsList();
		
		//天-城市-业务类型-数量
		Map<String,Map<String,Map<String,Integer>>> dayMap = new ConcurrentHashMap<String,Map<String,Map<String,Integer>>>();
		//天-城市-数量汇总
		Map<String,Map<String,Integer>> daySumMap = new HashMap<String,Map<String,Integer>>();
		//天-全国-类型-数量汇总
		Map<String,Map<String,Integer>> nationMap = new ConcurrentHashMap<String,Map<String,Integer>>();
		//存储数据城市代码
		Set<String> citySet = new HashSet<String>();
		for(Row row : list){
			String city_code = row.getString(0);
			String day = row.getString(1);
			Double num = row.getDouble(2);
			String type = row.getString(3);
			citySet.add(city_code);
			
			//具体类型计算
			Map<String,Map<String,Integer>> cityMap = dayMap.get(day);
			if(cityMap == null){
				cityMap = new HashMap<String,Map<String,Integer>>();
			}
			Map<String, Integer> typeMap = cityMap.get(city_code);
			if(typeMap == null){
				typeMap = new HashMap<String, Integer>();
			}
			Integer count = typeMap.get(type);
			if(count == null){
				count = 0;
			}
			count += num.intValue();
			typeMap.put(type, count);
			cityMap.put(city_code, typeMap);
			dayMap.put(day, cityMap);
			
			//天-城市-数量计算
			Map<String,Integer> citySumMap = daySumMap.get(day);
			if(citySumMap == null){
				citySumMap = new HashMap<String,Integer>();
			}
			Integer citySum = citySumMap.get(city_code);
			if(citySum == null){
				citySum = 0;
			}
			citySum += num.intValue();
			citySumMap.put(city_code, citySum);
			daySumMap.put(day, citySumMap);
			
			//天-全国-业务类型-数量计算
			Map<String,Integer> nationTypeMap = nationMap.get(day);
			if(nationTypeMap == null){
				nationTypeMap = new HashMap<String,Integer>();
			}
			Integer nationTypeCount = nationTypeMap.get(type);
			if(nationTypeCount == null){
				nationTypeCount = 0;
			}
			nationTypeCount += num.intValue();
			nationTypeMap.put(type, nationTypeCount);
			nationMap.put(day, nationTypeMap);
			
		}
		
		//向天-全国-类型数量中加入对应天的各城市数量，并加入所有城市汇总数量,完成nationMap组装
		for(String day : nationMap.keySet()){
			Map<String,Integer> nationTypeMap = nationMap.get(day); 
			Map<String,Integer> cityMap = daySumMap.get(day);
			
			for(String cityCode : cityMap.keySet()){
				//向全国-类型中加入每一个城市的汇总
				nationTypeMap.put(cityCode, cityMap.get(cityCode));
				//计算全国所有城市数量汇总，再放入全国类型Map
				Integer all = nationTypeMap.get("all");
				if(all == null){
					all = 0;
				}
				all += cityMap.get(cityCode);
				nationTypeMap.put("all", all);
			}
			nationMap.put(day, nationTypeMap);
		}
		
		//生成天-城市-类型数量中加入城市的汇总（all）,合并成天-城市-（汇总数量 ,各类型数量）,完成dayMap组装
		for(String day : dayMap.keySet()){
			Map<String,Map<String,Integer>> cityMap = dayMap.get(day);
			Map<String,Integer> citySumMap = daySumMap.get(day);
			for(String cityCode : cityMap.keySet()){
				Map<String,Integer> typeMap = cityMap.get(cityCode);
				Integer citySum = citySumMap.get(cityCode);
				typeMap.put("all", citySum);
				cityMap.put(cityCode, typeMap);
			}
			dayMap.put(day, cityMap);
		}
		
		//发布天-全国-（汇总数量，城市数量，类型数量）
		for(String day : nationMap.keySet()){
			Map<String,Integer> nationTypeMap = nationMap.get(day);
			countTickets(Constants.NATION_CODE, day, nationTypeMap);
		}
		
		//发布天-城市-类型（类型和全部）
		for(String day : dayMap.keySet()){
			Map<String,Map<String,Integer>> cityMap = dayMap.get(day);
			for(String cityCode : cityMap.keySet()){
				Map<String,Integer> typeMap = cityMap.get(cityCode);
				countTickets(cityCode, day, typeMap);
			}
		}
		//获取redis缓存的数据日期
		String last_date = LastTimeUtils.getLastTimeDate();
		for(String cityCode:citySet){
			String trandeKey = "trade:vol:" + cityCode;
			RMap<String, Map<String, Integer>> tradeMap = RedisUtils.getRedissonClient().getMap(trandeKey);
			processTicketsBydays(tradeMap,last_date,cityCode);
		}
		//////////////////xxhf和xfhx去重set添加开始//////////////////
		String endDate=endTime;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(DateUtils.parseDate(endDate));
		//计算3天
		calendar.add(Calendar.DATE, -3);
		String startDate = DateUtils.formatDate(calendar.getTime());
		Map<String,Object> setParams = new HashMap<String,Object>();
		setParams.put("startDate", startDate);
		if(endTime!=null){
			setParams.put("endDate", endDate);
		}
		setParams.put("lastTimestamp", lastTimestamp );
		//xxhf去重set设置
		Dataset<Row> xxhf_results = builder.executeSQL("xxhf_set", setParams);
		for(Row row : xxhf_results.collectAsList()){
			String seq = row.getString(0);
			String cardNum = row.getString(1);
			String cityCode = row.getString(2);
			String date = row.getString(3);
			RSet<String> xxhfSet = RedisUtils.getRedissonClient().getSet("order:uniq:" + "xxhf:" + cityCode + ":" + date);
			xxhfSet.add(seq+"_"+cardNum);
		}
		//xfhx去重set设置
		Dataset<Row> xfhx_results = builder.executeSQL("xfhx_set", setParams);
		for(Row row : xfhx_results.collectAsList()){
			String orderNo = row.getString(0);
			String cityCode = row.getString(1);
			String date = row.getString(2);
			RSet<String> xfhxSet = RedisUtils.getRedissonClient().getSet("order:uniq:" + "xfhx:" + cityCode + ":" + date);
			xfhxSet.add(orderNo);
		}
		
		
		//////////////////xxhf和xfhx去重set添加结束//////////////////
		builder.getSession().close();
		logger.info("=========================end==========================");
	}
	
	
		/**
		 * 统计
		 * @author senze
		 * @date 2018年1月24日 下午6:58:32
		 * @param type
		 * @param cityCode
		 * @param date
		 * @param ticketNum
		 * @param redisson
		 */
		public static void countTickets(String cityCode, String date, Map<String,Integer> detailMap){
			JSONObject json = new JSONObject();
			String trandeKey = "trade:vol:" + cityCode;
			RMap<String, Map<String, Integer>> tradeMap = RedisUtils.getRedissonClient().getMap(trandeKey);
			tradeMap.put(date, new HashMap<>());
			Map<String, Integer> tradeData = tradeMap.getOrDefault(date, new HashMap<>());
			Set<String> keys=detailMap.keySet();
			for(String key : keys){
				tradeData.put(key , detailMap.get(key));
			}
			try {
				tradeMap.put(date, tradeData);
			} catch (Exception e) {
				e.printStackTrace();
				logger.warn("错误数据：" + tradeData.toString()); 
				throw new RuntimeException(e);
			}
			Map<String, Integer> currentData = tradeMap.get(DateUtils.getCurrentDate());
			if (currentData != null) {
				for(String key : currentData.keySet()){
					json.put(key, currentData.get(key));
				}
				RedisUtils.getRedissonClient().getTopic(trandeKey).publish(json.toString());				//推送交易数据
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
				if(cacheMap.get(before)!=null){
					tickets.add(cacheMap.get(before).get("all")==null?0:cacheMap.get(before).get("all"));
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
		
}
