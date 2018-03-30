package com.shankephone.data.monitoring.web.device.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.shankephone.data.monitoring.web.device.dao.mysql.FailureInfoDao;
import com.shankephone.data.monitoring.web.device.model.FailureInfo;
import com.shankephone.data.monitoring.web.device.service.FailureInfoService;

@Service("failureInfoService")
public class FailureInfoServiceImpl implements FailureInfoService{
	
	private final static Logger logger = LoggerFactory
			.getLogger(FailureInfoServiceImpl.class);
	
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
	public Integer delete(FailureInfo fi) {
		return deleteByDeviceId(fi.getCity_code(), fi.getDevice_id(), fi.getFailure_type());
	}

	@Override
	public Map<String, Object> queryDeviceCountByCityCode(String city_code) {
		return failureInfoDao.queryDeviceCountByCityCode(city_code);
	}

	@Override
	public List<FailureInfo> queryListByCityCode(String city_code) {
		return failureInfoDao.queryListByCityCode(city_code);
	}

	@Override
	public List<Map<String, Object>> queryFailureTypeCountByCityCode(
			String city_code) {
		return failureInfoDao.queryFailureTypeCountByCityCode(city_code);
	}

	@Override
	public List<Map<String, Object>> queryStationDeviceCountByCityCode(
			String city_code) {
		return failureInfoDao.queryStationDeviceCountByCityCode(city_code);
	}

	@Override
	public List<Map<String, Object>> queryDeviceStatusByStationName(String city_code, String station_name) {
		return failureInfoDao.queryDeviceStatusByStationName(city_code, station_name);
	}

	@Override
	public List<Map<String, Object>> queryStationDeviceTypeCountByCityCode(
			String city_code) {
		return failureInfoDao.queryStationDeviceTypeCountByCityCode(city_code);
	}

	@Override
	public List<Map<String, Object>> queryFailureDeviceTypeCountByCityCode(
			String city_code) {
		return failureInfoDao.queryFailureDeviceTypeCountByCityCode(city_code);
	}

	@Override
	public Map<String, Object> queryDeviceTypeCountByCityCode(
			String city_code) {
		return failureInfoDao.queryDeviceTypeCountByCityCode(city_code);
	}

	@Override
	public List<Map<String, Object>> queryRtRecording(String city_code, String station_name, String device_id,
			String device_type, String status_value, String start_time, String end_time, int offset) {
		return failureInfoDao.queryRtRecording(city_code, station_name, device_id, device_type, status_value, start_time, end_time, offset);
	}
	
	public int countTotalNum(String city_code, String station_name, String device_id,
			String device_type, String status_value, String start_time, String end_time){
		return failureInfoDao.countTotalNum(city_code, station_name, device_id, device_type, status_value, start_time, end_time);
	}

	@Override
	public Map<String, Object> queryOfflineAndOtherRepeatCount() {
		return failureInfoDao.queryOfflineAndOtherRepeatCount();
	}


}
