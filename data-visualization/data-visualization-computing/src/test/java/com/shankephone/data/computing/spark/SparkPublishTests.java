package com.shankephone.data.computing.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.test.SpringTestCase;

public class SparkPublishTests extends SpringTestCase {
	
	@Test
	public void testPublish() throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		kafkaParams.put("group.id", "SparkPublishTests");
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("auto.commit.interval.ms", 1000);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		
		Collection<String> topics = Arrays.asList("hbase-order_info");
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkPublishTests");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
			jssc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			rdd.foreachPartition(p -> {
				try {
					while (p.hasNext()) {
						ConsumerRecord<String, String> record = p.next();
						JSONObject json = JSONObject.parseObject(record.key());
						String tableName = "wp-test";
						RedisUtils.getRedissonClient()
						          .getTopic(tableName)
						          .publish(record.key());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
		});
		jssc.start();
		jssc.awaitTermination();
	}
}
