package com.shankephone.data.monitoring.web.device.service.impl;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.web.device.dao.mysql.FailureDetailsDao;
import com.shankephone.data.monitoring.web.device.model.FailureDetails;
import com.shankephone.data.monitoring.web.device.service.FailureDetailsService;

@Service("failureDetailsService")
public class FailureDetailsServiceImpl implements FailureDetailsService {
	
	private final static Logger logger = LoggerFactory
			.getLogger(FailureDetailsServiceImpl.class);
	
	@Resource
	private FailureDetailsDao failureDetailsDao;

	@Override
	public void insert(FailureDetails fd) {
		failureDetailsDao.insert(fd);
	}

	@Override
	public void deleteByDeviceId(String cityCode, String deviceId) {
		failureDetailsDao.deleteByDeviceId(cityCode, deviceId);
	}

	@Override
	public void deleteByDeviceAndStatus(String cityCode, String deviceId, String statusId) {
		failureDetailsDao.deleteByDeviceAndStatus(cityCode, deviceId, statusId);
	}

	@Override
	public void delete(FailureDetails fd) {
		deleteByDeviceId(fd.getCity_code(), fd.getDevice_id());
	}

}
