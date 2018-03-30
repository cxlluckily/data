package com.shankephone.data.computing.kafka;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.kafka.KafkaHelper;

public class KafkaDataTest {
	
	public static void main(String[] args) {
		consumer();
	}
	
	public static void consumer(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		props.put("group.id", "11113");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("max.poll.records", 1);
		props.put("acks", "all");
		props.put("max.partition.fetch.bytes", 1);
		props.put("fetch.max.bytes", 1);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse("2017-09-25 00:00:00");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		KafkaHelper.setOffset((Map)props, "hbase-order_info", date.getTime());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("hbase-order_info"));
		out:while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				//System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
				String j = record.value();
				JSONObject json = JSONObject.parseObject(j);
				JSONObject vj = json.getJSONObject("columns");
				String status = vj.getString("TICKET_ORDER_STATUS");
				String takeTime = vj.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
				if("5".equals(status) && !"".equals(takeTime) && takeTime.compareTo("2017-09-25 00:02:56") == 0){
					System.out.println(record.key() + ":" + record.value());
					createProducer(record.key(), record.value());
					System.out.println(vj.toJSONString()); 
					System.out.println("成功!");
					break out;
				} 
				
			}
		}
	}
	
	public static void createProducer(String key, String value) {
		Properties props = new Properties();
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		//props.setProperty("client.id", "bug-test");
		props.setProperty("bootstrap.servers", 
				"data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		Producer<String, String> producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>("hbase-order_info", key, value),
	       		 (metadata, e) -> {
	       			 if (e != null) {
	       				 throw new RuntimeException(e);
	       			 }
	       		 });
		producer.close();
		
	}
	
	
}
