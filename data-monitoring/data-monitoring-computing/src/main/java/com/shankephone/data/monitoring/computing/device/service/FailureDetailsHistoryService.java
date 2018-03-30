package com.shankephone.data.monitoring.computing.device.service;

import java.util.List;
import java.util.Map;

import com.shankephone.data.monitoring.computing.device.model.FailureDetailsHistory;

public interface FailureDetailsHistoryService {

	void insert(FailureDetailsHistory fdp);

	void delete(FailureDetailsHistory fdp);
	
	List<Map<String,Object>> selectPK(String city_code, String device_id, String status_id);

	void recoverFailure(FailureDetailsHistory fdp);

	void updateStatus(FailureDetailsHistory fdp); 

}
