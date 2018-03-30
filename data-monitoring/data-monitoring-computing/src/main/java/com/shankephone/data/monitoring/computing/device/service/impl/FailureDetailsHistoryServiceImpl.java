package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.mysql.FailureDetailsHistoryDao;
import com.shankephone.data.monitoring.computing.device.model.FailureDetailsHistory;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsHistoryService;
@Service("failureDetailsHistoryService")
public class FailureDetailsHistoryServiceImpl implements FailureDetailsHistoryService{

	@Resource
	private FailureDetailsHistoryDao failureDetailsHistoryDao;
	
	@Override
	public void insert(FailureDetailsHistory fdp) {
		failureDetailsHistoryDao.insert(fdp);
	}

	@Override
	public void delete(FailureDetailsHistory fdp) {
		failureDetailsHistoryDao.deleteHistoryDetail(fdp);
	}

	@Override
	public List<Map<String, Object>> selectPK(String city_code, String device_id, String status_id) {
		return failureDetailsHistoryDao.selectPK(city_code, device_id, status_id);
	}

	@Override
	public void recoverFailure(FailureDetailsHistory fdp) {
		failureDetailsHistoryDao.recoverFailure(fdp);
	}

	@Override
	public void updateStatus(FailureDetailsHistory fdp) {
		failureDetailsHistoryDao.updateStatus(fdp);
	}

}
