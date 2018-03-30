package com.shankephone.data.monitoring.web.device.processor;

import java.text.SimpleDateFormat;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.monitoring.web.device.model.FailureInfo;
import com.shankephone.data.monitoring.web.device.service.FailureInfoService;
import com.shankephone.data.monitoring.web.device.service.impl.FailureInfoServiceImpl;

public class DeviceInitProcessor implements SubDataInitProcessor {
	
	private final static String topicPrefix = "device_failure_";

	@Override
	public Object process(SubInfo subInfo) {
		String topics = subInfo.getTopic();
		String city_code = topics.replace(topicPrefix, "");
		JSONArray array = new JSONArray();
		FailureInfoService failureInfoService = (FailureInfoServiceImpl)ContextAccessor.getBean("failureInfoService");
		List<FailureInfo> failureList = failureInfoService.queryListByCityCode(city_code);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (FailureInfo failure : failureList) {
			JSONObject json = new JSONObject();
			json.put("station_id", failure.getStation_code());
			json.put("station_name", failure.getStation_name());
			json.put("device_id", failure.getDevice_id());
			json.put("failure_time", sdf.format(failure.getFailure_time()));
			json.put("status_value", failure.getStatus_value());
			array.add(json);
		}
		return array;
	}

}
