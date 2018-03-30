package com.shankephone.data.monitoring.computing.device.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.computing.device.dao.mysql.FailureInfoDao;
import com.shankephone.data.monitoring.computing.device.model.FailureInfo;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoService;

@Service("failureInfoService")
public class FailureInfoServiceImpl implements FailureInfoService{
	
	/*private final static Logger logger = LoggerFactory
			.getLogger(FailureInfoServiceImpl.class);*/
	
	@Resource
	private FailureInfoDao failureInfoDao;

	@Override
	public void insert(FailureInfo fi) {
		failureInfoDao.insert(fi);
	}

	@Override
	public FailureInfo queryById(String cityCode, String deviceId) {
		return failureInfoDao.queryById(cityCode, deviceId);
	}

	@Override
	public void update(FailureInfo fi) {
		failureInfoDao.update(fi);
	}

	@Override
	public Integer deleteByDeviceId(String cityCode, String deviceId, String failureType) {
		return failureInfoDao.deleteByDeviceId(cityCode, deviceId, failureType);
	}

	@Override
	public List<FailureInfo> queryList() {
		return failureInfoDao.queryList();
	}

	@Override
	public List<Map<String, Object>> queryDeviceCount() {
		return failureInfoDao.queryDeviceCount();
	}

	@Override
	public Integer delete(FailureInfo fi) {
		return deleteByDeviceId(fi.getCity_code(), fi.getDevice_id(), fi.getFailure_type());
	}

	@Override
	public List<Map<String, Object>> queryFailureTypeCount() {
		return failureInfoDao.queryFailureTypeCount();
	}

	@Override
	public List<Map<String, Object>> queryStationDeviceCount() {
		return failureInfoDao.queryStationDeviceCount();
	}

	@Override
	public Map<String,Object> selectPK(String city_code, String device_id,
			String failure_type) {
		Map<String,Object> pkMap = failureInfoDao.selectPK(city_code, device_id, failure_type);
		return pkMap;
	}

	@Override
	public List<Map<String, Object>> queryDeviceTypeCount() {
		return failureInfoDao.queryDeviceTypeCount();
	}

	@Override
	public List<Map<String, Object>> queryFailureDeviceTypeCount() {
		return failureInfoDao.queryFailureDeviceTypeCount();
	}

	@Override
	public List<Map<String, Object>> queryStationDeviceTypeCount() {
		return failureInfoDao.queryStationDeviceTypeCount();
	}

	@Override
	public List<Map<String, Object>> queryFailureData(String city_code,
			String status_value) {
		return failureInfoDao.queryFailureData(city_code, status_value);
	}

	@Override
	public Map<String, Object> queryOfflineAndOtherRepeatCount() {
		return failureInfoDao.queryOfflineAndOtherRepeatCount();
	}

}
