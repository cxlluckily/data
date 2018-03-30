package com.shankephone.data.visualization.computing.user.offline;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
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
 * 订单来源分析
 * @author fengql
 * @version 2017年8月22日 下午5:06:09
 */
public class UserSourceProcessor implements Executable{
		
	private final static Logger logger = LoggerFactory.getLogger(UserSourceProcessor.class);
	
	@Override
	public void execute(Map<String, Object> argsMap) {
		logger.info("======================order-source-offline start===========================");
		analyseOthers("userSource",argsMap);
		logger.info("======================order-source-offline end===========================");
		
	}
	
	public void analyseOthers(String sqlFile, Map<String, Object> argsMap){
		logger.info("=========================analyseOthers start==========================");
		String startTime = argsMap == null ? null : argsMap.get("startTime") == null ? null : argsMap.get("startTime").toString();
		String endTime = argsMap == null ? null : argsMap.get("endTime") == null ? null : argsMap.get("endTime").toString();
		String lastTimestamp = argsMap == null ? null : argsMap.get("lastTimestamp") == null ? null : argsMap.get("lastTimestamp").toString();
		
		try {
			//结束时间
			endTime = DateUtils.formatDate(DateUtils.parseDate(endTime));
			//开始时间
			startTime = DateUtils.formatDate(DateUtils.parseDate(startTime));
			Date lastTime=DateUtils.parseDateTime(lastTimestamp);
			lastTimestamp=String.valueOf(lastTime.getTime());
			//pay_pay_time数据时间限制
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("startTime", startTime);
			params.put("endTime", endTime);
			params.put("lastTimestamp", lastTimestamp);
			
			SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
			Dataset<Row> rows = builder.executeSQL(sqlFile, params);
			
			processDataset(rows,builder.getSession());
			//发布xxhf的set
			Dataset<Row> xxhfRows = builder.executeSQL("userSoruceXxhfSet", params);
			for(Row r : xxhfRows.collectAsList()) {
				String seq = r.getString(0);
				String cardNum = r.getString(1);
				String cityCode = r.getString(2);
				String date = r.getString(3);
				RSet<String> xxhfSet = RedisUtils.getRedissonClient().getSet("user:source:uniq:xxhf:" + cityCode + ":" + date);
				xxhfSet.add(seq+"_"+cardNum);
			}
			
			builder.getSession().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("=========================analyseOthers end==========================");
	}
	
	public void processDataset(Dataset<Row> rows, SparkSession session){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		List<Row> list = rows.collectAsList();
		//存放数据的Map, 按城市，天，来源，数量存放
		Map<String,Map<String,Map<String,Integer>>> cityMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		for(Row r : list) {
			String cityCode = r.getString(0);
			String day = r.getString(1);
			String source = r.getString(2);
			Double num = r.getDouble(3);
			System.out.println("rows.collectAsList//////"+cityCode+"--"+day+"--"+source+"--"+num);
			if(cityCode == null || "".equals(cityCode)){
				continue;
			}
			if(day == null || "".equals(day)){
				continue;
			}
			if(source == null || "".equals(source)){
				continue;
			}
			Map<String,Map<String,Integer>> daysMap = cityMap.get(cityCode);
			if(daysMap == null){
				daysMap = new HashMap<String,Map<String,Integer>>();
			}
			Map<String,Integer> sourceMap = daysMap.get(day);
			if(sourceMap == null){
				sourceMap = new HashMap<String,Integer>();
			}
			//未找到对应来源，归为其它
			String sourceName = source;
			if(sourceName == null || "".equals(sourceName)){
				sourceName = "qt";
			}
			Integer count = sourceMap.get(sourceName);
			if(count == null){
				count = 0;
			}
			sourceMap.put(sourceName, num.intValue() + count);
			daysMap.put(day, sourceMap);
			cityMap.put(cityCode, daysMap);
		}
		
		//全部城市数据，按天、来源、数量存放
		Map<String,Map<String,Integer>> allMap = new HashMap<String,Map<String,Integer>>();
		String topic = Constants.REDIS_USER_SOURCE_TOPIC;
		Set<String> citySet = cityMap.keySet();
		Set<String> daySetFlag=new HashSet<String>();
		for(String cityCode : citySet){
			Map<String,Map<String,Integer>> daysMap = cityMap.get(cityCode);
			//按天写入缓存
			RMap<String,Map<String,Integer>> rm = redisson.getMap(topic + cityCode);
			RMap<String,Map<String,Integer>> allRm = redisson.getMap(topic + Constants.NATION_CODE);
			Set<String> daySet = daysMap.keySet();
			for(String day : daySet){
				rm.put(day, new HashMap<>());
				allRm.put(day, new HashMap<>());
				daySetFlag.add(day);
				//按天存放城市的数据
				try {
					rm.put(day, daysMap.get(day));
				} catch (Exception e) {
					JSONObject j = new JSONObject();
					j.putAll(daysMap.get(day));
					logger.error("错误数据：" + day + "," + j.toJSONString());
					throw new RuntimeException(e);
				}
				//获取某一天的全国来源数据
				Map<String,Integer> allSourceMap = allMap.get(day);
				//如果没有该天的全国来源数据，则创建来源数据的map
				if(allSourceMap == null || allSourceMap.size() == 0){
					//创建map
					allSourceMap = new HashMap<String,Integer>();
				}
				//获取当前城市该天的数据
				Map<String,Integer> citySourceMap = daysMap.get(day);
				Set<String> citySourceSet = citySourceMap.keySet();
				for(String source : citySourceSet){
					//获取当前来源的数量
					Integer count = citySourceMap.get(source);
					if(count == null){
						count = 0;
					}
					//获取全国当前来源的数量
					Integer allCount = allSourceMap.get(source);
					if(allCount == null){
						allCount = 0;
					}
					//计算全国来源的数量，该天、该来源数量的和
					allCount += count;
					allSourceMap.put(source, allCount);
					allMap.put(day, allSourceMap);
					allRm.put(day, allSourceMap);
				}
				//发布城市数据
				String date = LastTimeUtils.getLastTimeDate();
				if(day.equals(date)){
					Map<String,Integer> map = daysMap.get(day);
					String daysString = JSONObject.toJSONString(map);
					System.out.println(cityCode+":"+date+"--"+daysString);
					//logger.warn(cityCode + "--" + day + ", " + daysString);
					redisson.getTopic(topic + cityCode).publish(daysString);
				}
			}
		}
		
		//发布全国数据
		String date = LastTimeUtils.getLastTimeDate();
		Map<String,Integer> map = allMap.get(date);
		for(String day : daySetFlag){
			if(map != null && map.size() > 0){
				if(day.equals(date)){
					String allString = JSONObject.toJSONString(map);
					System.out.println("全国："+date+"--"+allString);
					//logger.warn("全国" + "--" + date + allString);
					redisson.getTopic(topic + Constants.NATION_CODE).publish(allString);
				}
			}
		}
		
		logger.info("--------发布成功！--------");
	}
}
