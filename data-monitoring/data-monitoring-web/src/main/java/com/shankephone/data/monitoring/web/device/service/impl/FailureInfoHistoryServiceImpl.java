package com.shankephone.data.monitoring.web.device.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.web.device.dao.mysql.FailureInfoHistoryDao;
import com.shankephone.data.monitoring.web.device.service.FailureInfoHistoryService;

@Service("failureInfoHistoryService")
public class FailureInfoHistoryServiceImpl implements FailureInfoHistoryService{

	@Resource
	private FailureInfoHistoryDao failureInfoHistoryDao;
	@Override
	public List<Map<String, Object>> queryFailureRank(String start_time, String city_code) {
		return failureInfoHistoryDao.queryFailureRank(start_time, city_code);
	}

}
