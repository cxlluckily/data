package com.shankephone.data.storage.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class KafkaConsumerTest {
	
	@Test
	public void startSyncTables() throws Exception{
		Properties props = new Properties();
		props.put("bootstrap.servers", "data1.test:6667,data2.test:6667,data3.test:6667,data4.test:6667,,data5.test:6667");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "latest");
		props.put("group.id", "ordertest1");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//		consumer.subscribe(Arrays.asList("hbase-order_info"));
		consumer.subscribe(Arrays.asList("order"));
//		consumer.subscribe(Arrays.asList("mysql-data-computing"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				String key = record.key();
				String value = record.value();
				System.out.println(key + "---" + value);
			}
		}
	}

}
