package com.shankephone.data.common.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.Test;

import com.shankephone.data.common.kafka.KafkaOffset;
import com.shankephone.data.common.test.SpringTestCase;

public class SparkStreamingBuilderTests extends SpringTestCase {
	
	@Test
	public void testSparkStreamingBuilder() {
		SparkStreamingBuilder builder = new SparkStreamingBuilder("test");
		String appName = builder.getConfig().getProperty("appName");
		assertEquals("test", appName);
	}
	
	@Test
	public void testCreateKafkaStream() throws InterruptedException {
		SparkStreamingBuilder builder = new SparkStreamingBuilder("test");
		JavaInputDStream<ConsumerRecord<String, String>> stream = builder.createKafkaStream(KafkaOffset.END);
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
		builder.getStreamingContext().start();
		builder.getStreamingContext().awaitTermination();
	}
	
}
