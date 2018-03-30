package com.shankephone.data.monitoring.computing.device.service;

import java.util.Map;

import com.shankephone.data.monitoring.computing.device.model.FailureInfoHistory;

public interface FailureInfoHistoryService {

	void insert(FailureInfoHistory failInfo);

	void delete(FailureInfoHistory failInfo);

	Map<String, Object> selectPK(String city_code, String device_id,
			String failure_type);

	void recoverFailure(FailureInfoHistory failInfo);

	void updateStatus(FailureInfoHistory failInfo); 

}
