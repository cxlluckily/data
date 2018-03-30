package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

import scala.Tuple2;
/**
 * 云闸机过闸统计（实时）
 * @author duxiaohua
 * @version 2018年3月12日 下午9:58:54
 */
public class GateTransitCountProcessor implements SparkStreamingProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(GateTransitCountProcessor.class);
	
	@Override
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args) {
		Map<String, Long> countMap = rdd.filter(record -> {
			try {
				String tableName = record.getString("tableName");
				if (!tableName.equals("SKP:DATA_YPT_TRAN")) {
					return false;
				}
				JSONObject changes = record.getJSONObject("changes");
				if (changes != null && !changes.isEmpty()) {
					return false;
				}
				JSONObject columns = record.getJSONObject("columns");
				String tranType = columns.getString("TRAN_TYPE");
				if (Constants.DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.contains(tranType)) {
					return true;
				}
				return false;
			} catch (Exception e) {
				e.printStackTrace();
				String message = "过滤云闸机数据异常：" + record.toJSONString();
				logger.error(message, e);
				throw new RuntimeException(message, e);
			}
		}).mapToPair(record -> {
			try {
				JSONObject columns = record.getJSONObject("columns");
				String productCategory = columns.getString("PRODUCT_CATEGORY");
				productCategory = Constants.getGateTransitTypeName(productCategory);
				String tranDate = columns.getString("TRAN_DATE");
				return new Tuple2<String, Integer>(tranDate + "_" + productCategory, 1);
			} catch (Exception e) {
				e.printStackTrace();
				String message = "分组云闸机数据异常：" + record.toJSONString();
				logger.error(message, e);
				throw new RuntimeException(message, e);
			}
		}).countByKey();
		
		Map<String, Map<String, Integer>> dateMap = new HashMap<>();
		for (Entry<String, Long> entry : countMap.entrySet()) {
			String date = entry.getKey().split("_")[0];
			date = DateUtils.convertDateStr(date, "yyyyMMdd");
			String type = entry.getKey().split("_")[1];
			Integer count = entry.getValue().intValue();
			Map<String, Integer> dateCountMap = dateMap.get(date);
			if (dateCountMap == null) {
				dateCountMap = new HashMap<>();
				dateMap.put(date, dateCountMap);
			}
			//统计总过闸数量
			Integer allCount = dateCountMap.get("all");
			if (allCount == null) {
				allCount = 0;
			}
			allCount = allCount + count;
			dateCountMap.put(type, count);
			dateCountMap.put("all", allCount);
		}

		RMap<String, Map<String, Integer>> rmap = RedisUtils.getRedissonClient().getMap(Constants.GATETRANSIT_REDIS_KEY);
		String lastTime = LastTimeUtils.getLastTimeDate();
		for (Entry<String, Map<String, Integer>> dateEntry : dateMap.entrySet()) {
			Map<String, Integer> dateCountMap = rmap.get(dateEntry.getKey());
			if (dateCountMap == null) {
				dateCountMap = dateEntry.getValue();
			} else {
				for (Entry<String, Integer> countEntry : dateEntry.getValue().entrySet()) {
					Integer count = dateCountMap.get(countEntry.getKey());
					if (count == null) {
						count = 0;
					}
					dateCountMap.put(countEntry.getKey(), (count + countEntry.getValue()));
				}
			}
			rmap.fastPut(dateEntry.getKey(), dateCountMap);
			if (dateEntry.getKey().equals(lastTime)) {
				RedisUtils.getRedissonClient().getTopic(Constants.GATETRANSIT_REDIS_KEY).publish(JSONObject.toJSONString(dateCountMap));
			}
		}
	}
	
}
