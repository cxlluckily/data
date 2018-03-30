package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.mysql.FailureDetailsDao;
import com.shankephone.data.monitoring.computing.device.model.FailureDetails;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsService;

@Service("failureDetailsService")
public class FailureDetailsServiceImpl implements FailureDetailsService {
	
	/*private final static Logger logger = LoggerFactory
			.getLogger(FailureDetailsServiceImpl.class);*/
	
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

	@Override
	public Map<String,Object> selectPK(String city_code, String device_id, String status_id) {
		Map<String,Object> pkMap = failureDetailsDao.selectPK(city_code, device_id, status_id);
		return pkMap;
	}

}
