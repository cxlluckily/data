package com.shankephone.data.monitoring.web.device.service;

import com.shankephone.data.monitoring.web.device.model.FailureDetails;

public interface FailureDetailsService {

	void insert(FailureDetails fd);

	void deleteByDeviceId(String cityCode, String deviceId);

	void deleteByDeviceAndStatus(String cityCode, String deviceId, String statusId);
	
	void delete(FailureDetails fd);

}
