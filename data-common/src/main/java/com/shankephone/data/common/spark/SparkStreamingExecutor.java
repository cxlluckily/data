package com.shankephone.data.common.spark;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.shankephone.data.common.kafka.KafkaOffset;
import com.shankephone.data.common.spark.config.StreamingApp;
import com.shankephone.data.common.spark.config.StreamingConfig;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.common.util.XmlUtils;

public class SparkStreamingExecutor {
	private final static Logger logger = LoggerFactory.getLogger(SparkStreamingExecutor.class);
	private final static String SPARK_STREAMING_CONIFG_FILE = "streamingConfig.xml";
	private final static String ARG_NAME_APPID = "appId";
	private final static String ARG_NAME_LASTTIMESTAMP = "lastTimestamp";
	private final static String ARG_NAME_OTHER = "args";
	
	public void execute(Map<String, Object> args) {
		String appId = (String) args.get(ARG_NAME_APPID);
		String startTimestamp = (String) args.get(ARG_NAME_LASTTIMESTAMP);
		if (StringUtils.isBlank(appId)) {
			throw new RuntimeException(ARG_NAME_APPID + "不能为空");
		}
		StreamingConfig config = XmlUtils.xml2Java(SPARK_STREAMING_CONIFG_FILE, StreamingConfig.class);
		StreamingApp app = config.getApp(appId);
		if (app == null) {
			throw new RuntimeException(SPARK_STREAMING_CONIFG_FILE + "中不存在Id为：" + appId + "的app。");
		}
		List<String> processors = app.getProcessors();
		if (processors.isEmpty()) {
			throw new RuntimeException(SPARK_STREAMING_CONIFG_FILE + "中Id为：" + appId + "的app，不存在任何processor");
		}
		SparkStreamingBuilder builder = new SparkStreamingBuilder(appId);
		List<SparkStreamingProcessor> processorList = new ArrayList<SparkStreamingProcessor>();
		for (String processor : processors) {
			try {
				Class<SparkStreamingProcessor> processorClass = (Class<SparkStreamingProcessor>) Class.forName(processor);
				SparkStreamingProcessor processorInstance = processorClass.newInstance();
				processorInstance.beforeProcess(builder.getStreamingContext(), args);
				processorList.add(processorInstance);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
		JavaInputDStream<ConsumerRecord<String, String>> ds = createKafkaStream(builder, startTimestamp);
		ds.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			JavaRDD<JSONObject> mapRdd = transformKafkaValue(rdd);
			JavaRDD<JSONObject> filteredRdd = filterRdd(mapRdd, startTimestamp);
			JavaRDD<JSONObject> cachedRdd = filteredRdd.cache();
			for (SparkStreamingProcessor processorInstance : processorList) {
				processorInstance.process(builder.getStreamingContext(), cachedRdd, args);
			}
			((CanCommitOffsets) ds.inputDStream()).commitAsync(offsetRanges);
		});
		builder.getStreamingContext().start();
		try {
			builder.getStreamingContext().awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public JavaRDD<JSONObject> transformKafkaValue(JavaRDD<ConsumerRecord<String, String>> rdd) {
		JavaRDD<JSONObject> resultRdd = rdd.map(f -> {
			String kafkaValue = f.value();
			try {
				return JSONObject.parseObject(kafkaValue);
			} catch (Exception e) {
				e.printStackTrace();
				String message = "解析JSON字符串异常：" + kafkaValue;
				logger.error(message, e);
				throw new RuntimeException(message, e);
			}
		});
		return resultRdd;
	}
	
	public JavaRDD<JSONObject> filterRdd(JavaRDD<JSONObject> rdd, String startTimestamp) {
		if (StringUtils.isBlank(startTimestamp) 
				|| startTimestamp.equalsIgnoreCase(KafkaOffset.START.value()) 
				|| startTimestamp.equalsIgnoreCase(KafkaOffset.END.value())) {
			return rdd;
		}
		long timestamp = DateUtils.parseDateTime(startTimestamp).getTime();
		JavaRDD<JSONObject> resultRdd = rdd.filter(f -> {
			try {
				JSONObject value = f.getJSONObject("columns");
				String lastTimestamp = value.getString("T_LAST_TIMESTAMP");
				return Long.parseLong(lastTimestamp) > timestamp;
			} catch (Exception e) {
				e.printStackTrace();
				String message = "按T_LAST_TIMESTAMP过滤数据异常：" + f.toJSONString();
				logger.error(message, e);
				throw new RuntimeException(message, e);
			}
		});
		return resultRdd;
	}
	
	public JavaInputDStream<ConsumerRecord<String, String>> createKafkaStream(SparkStreamingBuilder builder, String startTimestamp) {
		if (StringUtils.isBlank(startTimestamp)) {
			return builder.createKafkaStream();
		}
		if (startTimestamp.equalsIgnoreCase(KafkaOffset.START.value())) {
			return builder.createKafkaStream(KafkaOffset.START);
		}
		if (startTimestamp.equalsIgnoreCase(KafkaOffset.END.value())) {
			return builder.createKafkaStream(KafkaOffset.END);
		}
		long timestamp = DateUtils.parseDateTime(startTimestamp).getTime();
		return builder.createKafkaStream(timestamp);
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option appIdO = Option.builder(ARG_NAME_APPID)
											.required()
											.hasArg()
											.build();
		Option startTimeO = Option.builder(ARG_NAME_LASTTIMESTAMP)
											.hasArg()
											.build();
		Option argumentO = Option.builder(ARG_NAME_OTHER)
										   .hasArg()
										   .numberOfArgs(2)
										   .valueSeparator()
										   .build();
		options.addOption(appIdO);
		options.addOption(startTimeO);
		options.addOption(argumentO);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		String appId = cmd.getOptionValue(ARG_NAME_APPID);
		String lastTimestamp = cmd.getOptionValue(ARG_NAME_LASTTIMESTAMP);
		Map<String, Object> arguments = (Map)cmd.getOptionProperties(ARG_NAME_OTHER);
		arguments.put(ARG_NAME_APPID, appId);
		if (lastTimestamp != null) {
			arguments.put(ARG_NAME_LASTTIMESTAMP, lastTimestamp);
		}
		logger.info(ARG_NAME_OTHER + " : [" + arguments + "]");
		new SparkStreamingExecutor().execute(arguments);
	}
	
}
