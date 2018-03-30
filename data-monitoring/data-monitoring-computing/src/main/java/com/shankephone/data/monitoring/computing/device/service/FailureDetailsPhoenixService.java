package com.shankephone.data.monitoring.computing.device.service;

import java.util.List;

import com.shankephone.data.monitoring.computing.device.model.FailureDetailsPhoenix;

public interface FailureDetailsPhoenixService {

	public List<FailureDetailsPhoenix> select();
	
	public void insert(FailureDetailsPhoenix failInfo);
	
	public void deleteByPK(String PK);
	
	public String selectPK(String city_code, String device_id, String status_id);
}
