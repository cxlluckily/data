package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.phoenix.FailureDetailsPhoenixDao;
import com.shankephone.data.monitoring.computing.device.model.FailureDetailsPhoenix;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsPhoenixService;

@Service("failureDetailsPhoenixService")
public class FailureDetailsPhoenixServiceImpl implements FailureDetailsPhoenixService{

	@Resource FailureDetailsPhoenixDao failureDetailsPhoenixDao;
	@Override
	public List<FailureDetailsPhoenix> select() {
		return failureDetailsPhoenixDao.select();
	}

	@Override
	public void insert(FailureDetailsPhoenix failInfo) {
		failureDetailsPhoenixDao.insert(failInfo);
		
	}

	@Override
	public void deleteByPK(String PK) {
		failureDetailsPhoenixDao.deleteByPK(PK);
		
	}

	@Override
	public String selectPK(String city_code, String device_id, String status_id) {
		return failureDetailsPhoenixDao.selectPK(city_code, device_id, status_id);
	}

}
