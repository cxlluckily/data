package com.shankephone.data.common.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.util.PropertyAccessor;

/**
 * Kafka相关工具类
 * @author DuXiaohua
 * @version 2017年9月1日 上午10:38:32
 */
public class KafkaHelper {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaHelper.class);
	
	private static KafkaProducer<String, String> producer = null;
	
	private KafkaHelper(){}
	
	/**
	 * 设置下次消费时的offset，按时间戳自动查找对应的offset
	 * @author DuXiaohua
	 * @date 2017年9月1日 下午1:49:20
	 * @param consumer
	 * @param topic
	 * @param timestamp
	 */
	public static void setOffset(Map<String, Object> kafkaParams, String topic, long timestamp) {
		try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
			List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
			List<TopicPartition> topicPartitionList = new ArrayList<>();
			Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
			for (PartitionInfo info : partitionInfoList) {
		    	TopicPartition topicPartition = new TopicPartition(topic, info.partition());
		    	topicPartitionList.add(topicPartition);
		    	timestampsToSearch.put(topicPartition, timestamp);
		    }
		    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = consumer.offsetsForTimes(timestampsToSearch);
		    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = new HashMap<>();
		    for (Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestamp.entrySet()) {
		    	TopicPartition topicPartition = entry.getKey();
		    	long offset = entry.getValue().offset();
		    	OffsetAndMetadata offsetCommitted = consumer.committed(topicPartition);
		    	long oldOffset = offsetCommitted == null ? 0 : offsetCommitted.offset();
		    	offsetAndMetadata.put(topicPartition, new OffsetAndMetadata(offset));
		    	logger.info(String.format("按时间戳设置消费时的offset，topic：%s，分区：%s，新offset：%s，原offset：%s", topic, topicPartition.partition(), offset, oldOffset));
		    }
			consumer.commitSync(offsetAndMetadata);
		}
	}
	
    /**
     * 设置下次消费时的offset为kafka中起始的offset
     * @author DuXiaohua
     * @date 2017年9月1日 下午2:44:43
     * @param consumer
     * @param topic
     */
	public static void setOffsetToBeginning(Map<String, Object> kafkaParams, String topic) {
		try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
			List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
			List<TopicPartition> topicPartitionList = new ArrayList<>();
			for (PartitionInfo info : partitionInfoList) {
		    	topicPartitionList.add(new TopicPartition(topic, info.partition()));
		    }
			Map<TopicPartition, Long> partitionOffset = consumer.beginningOffsets(topicPartitionList);
			Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = new HashMap<>();
		    for (Entry<TopicPartition, Long> enrty : partitionOffset.entrySet()) {
		    	TopicPartition topicPartition = enrty.getKey();
		    	long offset = enrty.getValue();
		    	OffsetAndMetadata offsetCommitted = consumer.committed(topicPartition);
		    	long oldOffset = offsetCommitted == null ? 0 : offsetCommitted.offset();
		    	offsetAndMetadata.put(topicPartition, new OffsetAndMetadata(offset));
		    	logger.info(String.format("设置消费时的offset为起始值，topic：%s，分区：%s，新offset：%s，原offset：%s", topic, topicPartition.partition(), offset, oldOffset));
		    }
			consumer.commitSync(offsetAndMetadata);
		}
	}

    /**
     * 设置下次消费时的offset为kafka中最新的offset
     * @author DuXiaohua
     * @date 2017年9月1日 下午2:44:43
     * @param consumer
     * @param topic
     */
	public static void setOffsetToEnd(Map<String, Object> kafkaParams, String topic) {
		try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
			List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
			List<TopicPartition> topicPartitionList = new ArrayList<>();
			for (PartitionInfo info : partitionInfoList) {
		    	topicPartitionList.add(new TopicPartition(topic, info.partition()));
		    }
			Map<TopicPartition, Long> partitionOffset = consumer.endOffsets(topicPartitionList);
			Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = new HashMap<>();
		    for (Entry<TopicPartition, Long> enrty : partitionOffset.entrySet()) {
		    	TopicPartition topicPartition = enrty.getKey();
		    	long offset = enrty.getValue();
		    	OffsetAndMetadata offsetCommitted = consumer.committed(topicPartition);
		    	long oldOffset = offsetCommitted == null ? 0 : offsetCommitted.offset();
		    	offsetAndMetadata.put(topicPartition, new OffsetAndMetadata(offset));
		    	logger.info(String.format("设置消费时的offset为最新值，topic：%s，分区：%s，新offset：%s，原offset：%s", topic, topicPartition.partition(), offset, oldOffset));
		    }
			consumer.commitSync(offsetAndMetadata);
		}
	}
	
	/**
	 * 获取配置文件中的默认Kafka参数，以default.kafka开头
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:41:33
	 * @return
	 */
	public static Properties getKafkaParams() {
		return PropertyAccessor.getProperties("default.kafka");
	}
	
	/**
	 * 获取配置文件中Kafka Consumer默认参数，以default.consumer.kafka开头
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:42:03
	 * @return
	 */
	public static Properties getConsumerParams() {
		Properties defaultProps = getKafkaParams();
		Properties consumerDefaultProps = PropertyAccessor.getProperties("default.consumer.kafka");
		Properties finalProps = new Properties();
		finalProps.putAll(defaultProps);
		finalProps.putAll(consumerDefaultProps);
		return finalProps;
	}
	
	/**
	 * 获取配置文件中指定应用的Kafka Consumer参数，以appName.consumer.kafka开头
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:42:41
	 * @param appName
	 * @return
	 */
	public static Properties getConsumerParams(String appName) {
		Properties appProps = PropertyAccessor.getProperties(appName + ".consumer.kafka");
		Properties finalProps = getConsumerParams();
		finalProps.putAll(appProps);
		return finalProps;
	}
	
	/**
	 * 获取配置文件中Kafka Producer默认参数，以default.producer.kafka开头
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:44:11
	 * @return
	 */
	public static Properties getProducerParams() {
		Properties defaultProps = getKafkaParams();
		Properties producerDefaultProps = PropertyAccessor.getProperties("default.producer.kafka");
		Properties finalProps = new Properties();
		finalProps.putAll(defaultProps);
		finalProps.putAll(producerDefaultProps);
		return finalProps;
	}
	
	/**
	 * 获取配置文件中指定应用的Kafka Producer参数，以appName.producer.kafka开头
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:44:55
	 * @param appName
	 * @return
	 */
	public static Properties getProducerParams(String appName) {
		Properties appProps = PropertyAccessor.getProperties(appName + ".producer.kafka");
		Properties finalProps = getProducerParams();
		finalProps.putAll(appProps);
		return finalProps;
	}
	
	/**
	 * 以默认参数创建Kafka Consumer
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:45:23
	 * @return
	 */
	public static KafkaConsumer<String, String> createConsumer() {
		return new KafkaConsumer<>(getConsumerParams());
	}
	
	/**
	 * 以指定应用的参数创建Kafka Consumer
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:45:52
	 * @param appName
	 * @return
	 */
	public static KafkaConsumer<String, String> createConsumer(String appName) {
		return new KafkaConsumer<>(getConsumerParams(appName)); 
	}
	
	/**
	 * 以默认参数创建Kafka Producer
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:46:17
	 * @return
	 */
	public static KafkaProducer<String, String> createProducer() {
		return new KafkaProducer<>(getProducerParams());
	}
	
	/**
	 * 以指定应用的参数创建Kafka Producer
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:46:30
	 * @param appName
	 * @return
	 */
	public static KafkaProducer<String, String> createProducer(String appName) {
		return new KafkaProducer<>(getProducerParams(appName));
	}
	
	public synchronized static KafkaProducer<String, String> buildProducer(String appName) {
		if(producer == null){
			producer = new KafkaProducer<>(getProducerParams(appName));
		}
		return producer;
	}
	
}
