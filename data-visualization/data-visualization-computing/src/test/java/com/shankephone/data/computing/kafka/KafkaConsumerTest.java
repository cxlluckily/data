package com.shankephone.data.computing.kafka;

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
		consume();
	}
	
	public static void consume() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		props.put("group.id", "errortest1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.put("max.poll.records", 1);
		//props.put("max.partition.fetch.bytes", 1);
		//props.put("fetch.max.bytes", 1);		   
		KafkaHelper.setOffset((Map)props, "order", 1521021000000l);
		KafkaConsumer consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("order"));
		out:while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			int i = 0;
			for (ConsumerRecord<String, String> record : records){
				//String key = record.key();
				//JSONObject json = JSONObject.parseObject(key);
				//String schemaName = json.getString("schemaName");
				//String tableName = json.getString("tableName");
				//if (schemaName.equalsIgnoreCase("STTRADE4401") && tableName.equalsIgnoreCase("OWNER_ORDER_SINGLE_TICKET")) {
					//System.out.println(record.key() + ":" + record.value());
					//if(record.value().contains("01201803011114541728")){
						System.out.println(record.key() + ":" + record.value());
					//}
				//}
			}
		}
	}
}
