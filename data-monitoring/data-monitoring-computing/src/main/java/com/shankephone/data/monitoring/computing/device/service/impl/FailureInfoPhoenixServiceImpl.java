package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.phoenix.FailureInfoPhoenixDao;
import com.shankephone.data.monitoring.computing.device.model.FailureInfoPhoenix;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoPhoenixService;

@Service("failureInfoPhoenixService")
public class FailureInfoPhoenixServiceImpl implements FailureInfoPhoenixService{

	@Resource
	private FailureInfoPhoenixDao failureInfoPhoenixDao;
	
	@Override
	public List<FailureInfoPhoenix> select() {
		return failureInfoPhoenixDao.select();
	}

	@Override
	public void insert(FailureInfoPhoenix failInfo) {
		failureInfoPhoenixDao.insert(failInfo);
	}

	@Override
	public void deleteByPK(String PK) {
		failureInfoPhoenixDao.deleteByPK(PK);		
	}

	@Override
	public String selectPK(String city_code, String device_id, String failure_type) {
		return failureInfoPhoenixDao.selectPK(city_code, device_id, failure_type);
	}

	@Override
	public List<Map<String, Object>> queryFailureFrequence(String start_time) {
		return failureInfoPhoenixDao.queryFailureFrequence(start_time);
	}

}
