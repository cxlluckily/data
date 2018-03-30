package com.shankephone.data.collecting.config;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import com.shankephone.data.common.kafka.KafkaHelper;
import com.shankephone.data.common.test.TestCase;

public class KafkaTest extends TestCase{
	@Test
	public void test() {
		Producer<String, String> producer = KafkaHelper.createProducer();
		System.out.println(producer.toString());
	}
	
	@Test
	public void ConsumerTest() {
		
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(10000);
		System.out.println(records.count());
		for (ConsumerRecord<String, String> record : records) 
			System.out.println("offset = "+ record.offset()+"【partition】 = "+ record.partition()+" key = "+record.key()+" value = "+record.value());
		}
		
	}
	
	public static KafkaConsumer<String, String> createKafkaConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		props.put("group.id", "c1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("storage"));
		return consumer;
	}
	

}
