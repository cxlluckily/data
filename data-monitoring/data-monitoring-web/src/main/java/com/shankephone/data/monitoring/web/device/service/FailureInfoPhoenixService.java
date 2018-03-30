package com.shankephone.data.monitoring.web.device.service;

import java.util.List;
import java.util.Map;

import com.shankephone.data.monitoring.web.device.model.FailureInfoPhoenix;


public interface FailureInfoPhoenixService {
	
	public List<FailureInfoPhoenix> select();
	
	public void insert(FailureInfoPhoenix failInfo);
	
	public void deleteByPK(String PK);
	
	public String selectPK(String city_code, String device_id, String failure_type);
	
	public List<Map<String, Object>> queryFailureFrequence(String start_time, String city_code);

}
