package com.shankephone.data.common.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import com.shankephone.data.common.spark.SparkStreamingBuilderTests;
import com.shankephone.data.common.test.SpringTestCase;

public class KafkaHelperTests extends SpringTestCase {
	
	@Test
	public void testGetConsumerParams() {
		Properties props = KafkaHelper.getConsumerParams(SparkStreamingBuilderTests.class.getSimpleName());
		System.out.println(props);
		assertEquals("SparkStreamingBuilderTests", props.getProperty("group.id"));
	}
	
	@Test
	public void testGetProducerParams() {
		Properties props = KafkaHelper.getProducerParams("canal");
		System.out.println(props);
		assertEquals("canal", props.getProperty("client.id"));
	}
	
	@Test
	public void testCreateConsumer() {
		KafkaConsumer<String, String> consumer = KafkaHelper.createConsumer(SparkStreamingBuilderTests.class.getSimpleName());
		assertNotNull(consumer);
	}

	@Test
	public void testCreateProducer() {
		KafkaProducer<String, String> producer = KafkaHelper.createProducer("canal");
		assertNotNull(producer);
	}
	
}
