package com.shankephone.data.visualization.computing.user.offline;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;

/**
 * 新老用户离线分析
 * @author fengql
 * @version 2018年2月2日 下午3:55:55
 */
public class UserActiveProcessor implements Executable{

	private final static Logger logger = LoggerFactory.getLogger(UserActiveProcessor.class);
	
	@Override
	public void execute(Map<String, Object> argsMap) {
		logger.info("======================新老用户离线任务开始....===========================");
		SparkSQLBuilder builder = new SparkSQLBuilder("user_action_offline");
		analyseActivityUsers(builder, "userNewRegister", Constants.ANALYSE_USER_NUM_NEW, argsMap);
		analyseActivityUsers(builder, "userOldPayment", Constants.ANALYSE_USER_NUM_OLD, argsMap);
		builder.getSession().close();
		logger.info("======================新老用户离线任务结束===========================");
	}
	
	public void analyseActivityUsers(SparkSQLBuilder builder,  String sqlFile, String category, Map<String, Object> argsMap){
		try {
			String timestamp = argsMap == null ? null : argsMap.get("lastTimestamp") == null ? null : argsMap.get("lastTimestamp").toString();
			//t_last_timestamp的时间戳
			String endtime = DateUtils.getTimestamp(timestamp);
			if(endtime == null || "".equals(endtime)){
				Date date = new Date();
				endtime = date.getTime() + "";
			}
			Long end = Long.parseLong(endtime);
			//前一天的最后时刻时间戳
			String endtimestamp = DateUtils.getLastDateTerminal(timestamp, 1);
			long dataend = Long.parseLong(endtimestamp);
			Date date = new Date(dataend);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.DAY_OF_YEAR, -30);
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("startDate", "'" + DateUtils.formatDate(cal.getTime()) + "'");
			params.put("endDate", "'" + DateUtils.formatDate(date) + "'");
			Dataset<Row> results = builder.executeSQL(sqlFile, params);
			reduceByPeriodType(end, results, category);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * 按时间维度合并计算
	 * @author fengql
	 * @date 2017年8月22日 下午4:58:34
	 * @param periodType
	 * @param dataset
	 * @param category
	 */
	private void reduceByPeriodType(Long end,
			 Dataset<Row> dataset, String category) {
		Date date = new Date(end);
		//今天的时间与上面的date不是同一天，date是昨天
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String, Map<String,Integer>> rmap = redisson.getMap(
				Constants.REDIS_NAMESPACE_USER_ACTIVE
				);
		//生成list结果集
		List<Row> rows = dataset.collectAsList();
		//key:城市_用户类型,value:JSONObject<day, count>的map
		Map<String, Map<String,Integer>> map = new HashMap<String,Map<String,Integer>>();
		
		//每个城市-用户类型计算结果中的日期列表
		Map<String,Set<String>> daySetMap = new HashMap<String,Set<String>>();
		for(Row r : rows){
			String city_code = r.getString(0);
			String day = r.getString(1);
//			String day = dates.split(" ")[0];
			Long count = r.getLong(2);
			String key = city_code + Constants.DIMENSION_SEPERATOR + category;
			//存放day和count的JSONObject
			Map<String,Integer> mapDayCount = map.get(key);
			if(mapDayCount == null){
				mapDayCount = new HashMap<String, Integer>();
			}
			mapDayCount.put(day, count.intValue());
			map.put(key, mapDayCount);
			Set<String> daySet = daySetMap.get(key);
			if(daySet == null){
				daySet = new HashSet<String>();
			}
			//放入计算出的天
			daySet.add(day);
			daySetMap.put(key, daySet);
		}
		
		//生成30天的日期
		List<String> list = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		for(int i = 0; i < 30; i++){
			cal.add(Calendar.DAY_OF_YEAR, -1);
			Date day = cal.getTime();
			String days = DateUtils.formatDate(day);
			list.add(days);
		}
		Collections.reverse(list);
		
		Map<String, Map<String,Integer>> allMap = new HashMap<String, Map<String,Integer>>();
		//遍历30天日期，补齐并数据
		for(String day : list){
			System.out.println("list--------------"+day);
			Set<String> keySet = map.keySet();
			for(String key : keySet){
				Map<String,Integer> mapDayCount = map.get(key);
				if(mapDayCount == null){
					continue;
				}
				Set<String> days = daySetMap.get(key);
				//如果没有当前日期，补齐数据，放回map
				if(!days.contains(day)){
					mapDayCount.put(day, 0);
					map.put(key, mapDayCount);
				}
				int val = mapDayCount.get(day);
				//取出汇总的JSON
				Map<String,Integer> allMapDayCount = allMap.get(Constants.NATION_CODE + 
						Constants.DIMENSION_SEPERATOR + category);
				if(allMapDayCount == null){
					allMapDayCount = new HashMap<String, Integer>();
				}
				int allVal = allMapDayCount.get(day)==null?0:allMapDayCount.get(day);
				allMapDayCount.put(day, allVal + val);
				allMap.put(Constants.NATION_CODE + 
						Constants.DIMENSION_SEPERATOR + category, allMapDayCount);
			}
		}
		
		
		//遍历城市-用户类型的map，并放入缓存	
		Set<String> keySet = map.keySet();
		for(String key : keySet){
			Map<String,Integer> mapDayCount = map.get(key);
			String city_code = key.split(Constants.DIMENSION_SEPERATOR)[0];
			String cacheName = Constants.REDIS_NAMESPACE_USER_ACTIVE;
			//写入缓存
			rmap.put(key, mapDayCount);
			JSONObject o = getAllUserActive(cacheName, city_code,list);
			if(o != null){
				redisson.getTopic(
						Constants.REDIS_NAMESPACE_USER_ACTIVE + ":" + city_code
						).publish(o.toJSONString());
			} 
		}
		//遍历all-用户类型的map，并放入缓存
		Set<String> allKeyset = allMap.keySet();
		for(String key : allKeyset){
			Map<String,Integer> allMapDayCount = allMap.get(key);
			//写入缓存
			rmap.put(key, allMapDayCount);
			String cacheName = Constants.REDIS_NAMESPACE_USER_ACTIVE ;
			JSONObject o = getAllUserActive(cacheName, Constants.NATION_CODE,list); 
			if(o != null){
				redisson.getTopic(
						Constants.REDIS_NAMESPACE_USER_ACTIVE + ":" + Constants.NATION_CODE		
						).publish(o.toJSONString());
			}
		}
	}
	
	private JSONObject getAllUserActive(String cacheName ,String city_code,List<String> list){
		RedissonClient redisson = RedisUtils.getRedissonClient(); 
		RMap<String, Map<String,Integer>> rm = redisson.getMap(cacheName);
		Map<String,Integer> oldstr = rm.get(city_code + Constants.DIMENSION_SEPERATOR + "2200");
		Map<String,Integer> newstr = rm.get(city_code + Constants.DIMENSION_SEPERATOR + "2100");
		JSONObject json = new JSONObject();
		JSONArray xaxis = new JSONArray();
	
		if(oldstr != null && !"".equals(oldstr)){ 
			JSONArray oldArray = new JSONArray();
			for (int i = 0; i < list.size(); i++) {
				String day = list.get(i);
				System.out.println("oldList----------"+day);
				if(oldstr.get(day)==null){
					if(i!=0 && i!=list.size()-1){
						oldArray.add(0);
					}
				}else{
					oldArray.add(oldstr.get(day));
				}
			}
			json.put("olds", oldArray);
			
		}
		
		if(newstr != null && !"".equals(newstr)){ 
			JSONArray newArray = new JSONArray();
			for (int i = 0; i < list.size(); i++) {
				String day = list.get(i);
				System.out.println("newList----------"+day);
				if(newstr.get(day)==null){
					if(i!=0 && i!=list.size()-1){
						newArray.add(0);
					}
				}else{
					newArray.add(newstr.get(day));
				}
				xaxis.add(day);
			}
			json.put("news", newArray);
		}
		json.put("xaxis", xaxis);
			
		
		return json;
	}

}
