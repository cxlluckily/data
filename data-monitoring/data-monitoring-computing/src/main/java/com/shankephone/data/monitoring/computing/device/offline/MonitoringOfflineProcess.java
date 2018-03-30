package com.shankephone.data.monitoring.computing.device.offline;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.monitoring.computing.device.offline.FailureCountOffline;

public class MonitoringOfflineProcess implements Executable {
	private static final Logger logger = LoggerFactory.getLogger(MonitoringOfflineProcess.class);
	@Override
	public void execute(Map<String, Object> args) {
		String startTime = (String) args.get("startTime");
		String endTime = (String) args.get("endTime");
		String city_code = (String) args.get("city_code");
		
		SparkSQLBuilder builder = new SparkSQLBuilder("monitoring_offline");

		FailureCountOffline failureCountOffline = new FailureCountOffline();
		failureCountOffline.makeupHistFailure(builder, city_code, startTime, endTime);
		logger.info("-----------------【end of makeupHistFailure】--------------------");
		failureCountOffline.makeupHistOffline(builder, city_code, startTime, endTime);
		logger.info("-----------------【end of makeupHistOffline】--------------------");
		builder.getSession().stop();
		builder.getSession().close();
	}
}
