package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.mysql.FailureInfoHistoryDao;
import com.shankephone.data.monitoring.computing.device.model.FailureInfoHistory;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoHistoryService;

@Service("failureInfoHistoryService")
public class FailureInfoHistoryServiceImpl implements FailureInfoHistoryService {

	@Resource
	private FailureInfoHistoryDao failureInfoHistoryDao;
	
	@Override
	public void insert(FailureInfoHistory failInfo) {
		failureInfoHistoryDao.insert(failInfo);
	}

	@Override
	public void delete(FailureInfoHistory failInfo) {
		failureInfoHistoryDao.deleteByDeviceId(failInfo);
		
	}

	@Override
	public Map<String, Object> selectPK(String city_code, String device_id,
			String failure_type) {
		return failureInfoHistoryDao.selectPK(city_code, device_id, failure_type);
	}

	@Override
	public void recoverFailure(FailureInfoHistory failInfo) {
		failureInfoHistoryDao.recoverFailure(failInfo);
	}

	@Override
	public void updateStatus(FailureInfoHistory failInfo) {
		failureInfoHistoryDao.updateStatus(failInfo);
	}

}
