package com.shankephone.data.monitoring.computing.device.streaming;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.kafka.KafkaOffset;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.common.util.PropertyAccessor;

import scala.Tuple2;

public class TransXXHFProcessor {
	
	/*操作类型*/
	public static Map<String,String> transTypeMap = new HashMap<String,String>();
	public static final String TRANS_TYPE_ENTRY = "entry";  	
	public static final String TRANS_TYPE_EXIT = "exit";		
	public static final String TRANS_TYPE_SUPPLEMENT = "supplement";	
	public static final String TRANS_TYPE_SUCCESS = "success";	
	static {
		transTypeMap.put("53", TRANS_TYPE_ENTRY);				//进站
		transTypeMap.put("54", TRANS_TYPE_EXIT);					//出站
		transTypeMap.put("56", TRANS_TYPE_SUPPLEMENT);	//补票
		transTypeMap.put("0", TRANS_TYPE_SUCCESS);				//扣款成功
	}
	
	public void context(String appId, KafkaOffset offset, int windowDuration, int slideDuration) {
		SparkStreamingBuilder builder = new SparkStreamingBuilder(appId);
		builder.getStreamingContext().checkpoint(PropertyAccessor.getProperty("checkpoint.dir"));
		JavaInputDStream<ConsumerRecord<String, String>> ds = builder.createKafkaStream(offset);
		
		excute(ds, windowDuration, slideDuration);
		
		builder.getStreamingContext().start();
		try {
			builder.getStreamingContext().awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public void excute(JavaInputDStream<ConsumerRecord<String, String>> ds, int windowDuration, int slideDuration) {
		ds.map(rdd -> {
			return JSONObject.parseObject(rdd.value());
		}).filter(rdd -> {
			if ("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(rdd.getString("tableName"))) {
				return true;
			} else if ("SKP:METRO_MEMBER_DEBIT_REQUEST_RECORD".equals(rdd.getString("tableName"))) {
				JSONObject data = rdd.getJSONObject("columns");
				if ("0".equals(data.getString("DEBIT_REQUEST_RESULT"))) {
					return true;
				}
			}
			return false;
		}).mapToPair(rdd -> {
			JSONObject data = rdd.getJSONObject("columns");
			String cityCode = data.getString("CITY_CODE");
			String trxType = data.getString("TRX_TYPE");
			String result = trxType == null ? data.getString("DEBIT_REQUEST_RESULT") : trxType;
			String key = transTypeMap.get(result) + ":" + cityCode;
			return new Tuple2<String, Integer>(key, 1);
		}).reduceByKeyAndWindow((a, b) -> a + b,
													 (a, b) -> a - b,
													 Durations.minutes(windowDuration), 
													 Durations.minutes(slideDuration))
		.foreachRDD(rdd -> {			
			String[] date = DateUtils.getCurrentDate("yyyy-MM-dd HH:mm:00").split(" ");
			String currentDay = date[0];
			String currentTime = date[1];
			String namespace = "trans:xxhf:";
			rdd.foreachPartition(part -> {
				RedissonClient redisson = RedisUtils.getRedissonClient();
				while (part.hasNext()) {
					Tuple2<String, Integer> data = part.next();
					RMap<String, Map<String, Integer>> transMap = redisson.getMap(namespace + data._1);
					
					Map<String, Integer> timeMap = transMap.get(currentDay);
					if (timeMap == null) {
						timeMap = new ConcurrentHashMap<String,Integer>();
					}
					timeMap.put(currentTime, data._2);
					transMap.put(currentDay, timeMap);

					JSONObject json = new JSONObject();
					json.put("type", "add");
					json.put("day", currentDay);
					json.put("time", currentTime);
					String[] temp = {currentTime, data._2 + ""};
					json.put("value", temp);
					redisson.getTopic(namespace + data._1).publish(json.toJSONString());
//					System.out.println(namespace + data._1 + " " + json.toString());
				}
			});
		});
	}
	
	public static void main(String[] args) {
		new TransXXHFProcessor().context("transXXHFProcessor", KafkaOffset.END, 10, 1);
	}
}
