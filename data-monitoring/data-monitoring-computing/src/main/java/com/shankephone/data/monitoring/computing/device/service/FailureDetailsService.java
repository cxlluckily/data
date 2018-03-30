package com.shankephone.data.monitoring.computing.device.service;

import java.util.Map;

import com.shankephone.data.monitoring.computing.device.model.FailureDetails;

public interface FailureDetailsService {

	void insert(FailureDetails fd);

	void deleteByDeviceId(String cityCode, String deviceId);

	void deleteByDeviceAndStatus(String cityCode, String deviceId, String statusId);
	
	void delete(FailureDetails fd);

	Map<String,Object> selectPK(String city_code, String device_id, String status_id);

}
