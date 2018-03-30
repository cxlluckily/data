package com.shankephone.data.common.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.shankephone.data.common.kafka.KafkaHelper;
import com.shankephone.data.common.kafka.KafkaOffset;
import com.shankephone.data.common.util.PropertyAccessor;

/**
 * SparkStreaming相关的构造器
 * @author DuXiaohua
 * @version 2017年9月21日 下午1:37:52
 */
public class SparkStreamingBuilder extends SparkBuilder {
	
	private JavaStreamingContext streamingContext;
	
	public SparkStreamingBuilder(Class clazz) {
		super(clazz);
	}
	
	public SparkStreamingBuilder(String appName) {
		super(appName);
	}
	
	protected void mergeConfig() {
		super.mergeConfig();
		Properties sparkStreamingConfig = PropertyAccessor.getProperties("default.streaming.spark");
		Properties appConfig = PropertyAccessor.getProperties(appName + ".streaming.spark");
		config.putAll(sparkStreamingConfig);
		config.putAll(appConfig);
	}

	/**
	 * 获取JavaStreamingContext，如果没有则创建:
	 * 配置文件中的master对应SparkConf.setMaster;
	 * 配置文件中的appName对应SparkConf.setAppName，如果未配置取配置文件中此应用的标识，appName自动增加后缀.Streaming
	 * 配置文件中的duration对应JavaStreamingContext构造方法的Duration，单位为毫秒。
	 * @author DuXiaohua
	 * @date 2017年9月21日 下午4:24:08
	 * @return
	 */
	public JavaStreamingContext getStreamingContext() {
		if (streamingContext == null) {
			SparkConf conf = new SparkConf().setMaster(config.getProperty("master"))
					                        .setAppName(realAppName + ".Streaming")
					                        .set("spark.streaming.stopGracefullyOnShutdown", "true");
			streamingContext = new JavaStreamingContext(conf, Durations.milliseconds(Long.parseLong(config.getProperty("duration"))));
		}
		return streamingContext;
	}
	
	/**
	 * 根据配置文件创建Kafka JavaInputDStream：
	 * kafka参数在配置文件中需要以appName.consumer.kafka开头，如未配置取默认值；
	 * kafka topic在配置文件中为topics，可以为多个中间以,分隔
	 * @author DuXiaohua
	 * @date 2017年9月21日 下午4:32:33
	 * @return
	 */
	public JavaInputDStream<ConsumerRecord<String, String>> createKafkaStream() {
		return KafkaUtils.createDirectStream(
					getStreamingContext(),
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(getKafkaTopic(), getKafkaParams())
				  );
	}
	
	/**
	 * 根据配置文件创建Kafka JavaInputDStream：
	 * kafka参数在配置文件中需要以appName.consumer.kafka开头，如未配置取默认值；
	 * kafka topic在配置文件中为topics，可以为多个中间以,分隔
	 * @author DuXiaohua
	 * @date 2017年9月21日 下午4:47:15
	 * @param timestamp 用指定时间戳重新设置kafka消费的开始offset
	 * @return
	 */
	public JavaInputDStream<ConsumerRecord<String, String>> createKafkaStream(long timestamp) {
		for (String topic : getKafkaTopic()) {
			KafkaHelper.setOffset(getKafkaParams(), topic, timestamp);
		}
		return createKafkaStream();
	}
	
	/**
	 * 根据配置文件创建Kafka JavaInputDStream：
	 * kafka参数在配置文件中需要以appName.consumer.kafka开头，如未配置取默认值；
	 * kafka topic在配置文件中为topics，可以为多个中间以,分隔
	 * @author DuXiaohua
	 * @date 2017年9月21日 下午4:58:17
	 * @param KafkaOffset 需要设置的Kafka Offset枚举
	 * @return
	 */
	public JavaInputDStream<ConsumerRecord<String, String>> createKafkaStream(KafkaOffset kafkaOffset) {
		for (String topic : getKafkaTopic()) {
			if (kafkaOffset == KafkaOffset.START) {
				KafkaHelper.setOffsetToBeginning(getKafkaParams(), topic);
			} else if (kafkaOffset == KafkaOffset.END) {
				KafkaHelper.setOffsetToEnd(getKafkaParams(), topic);
			}
		}
		return createKafkaStream();
	}
	
	private Map<String, Object> getKafkaParams() {
		Properties params = KafkaHelper.getConsumerParams(appName);
		if (StringUtils.isBlank(params.getProperty("group.id"))) {
			params.setProperty("group.id", realAppName);
		}
		return (Map)params;
	}
	
	private List<String> getKafkaTopic() {
		String topics = config.getProperty("topics");
		String[] topicArray = topics.split(",");
		return Arrays.asList(topicArray);
	}
	
}
