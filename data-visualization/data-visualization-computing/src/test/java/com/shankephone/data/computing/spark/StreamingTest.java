package com.shankephone.data.computing.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class StreamingTest {
	public static void main(String[] args) throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "data1.test:6667,data2.test:6667,data3.test:6667");
		kafkaParams.put("group.id", "StreamingTest123");
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("auto.commit.interval.ms", 1000);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		
		Collection<String> topics = Arrays.asList("mysql-storage");
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StreamingTest");
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
					OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
					while (p.hasNext()) {
						ConsumerRecord<String, String> record = p.next();
						System.out.println(System.currentTimeMillis() + "=============" + TaskContext.get().partitionId() + ":" + record.partition() + ":" + record.offset() + ":" + o.fromOffset() + ":" + o.untilOffset());
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