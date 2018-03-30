package com.shankephone.data.computing.spark;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.kafka.KafkaHelper;

public class KafkaConsumerTest {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		props.put("group.id", "tradeCount");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("max.poll.records", 1);
		props.put("max.partition.fetch.bytes", 1);
		props.put("fetch.max.bytes", 1);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("order"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				System.out.println(record.key() + " : " + record.value());
			}
		}
	}
}



