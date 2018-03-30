package com.shankephone.data.monitoring.web.device.service;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.web.device.model.FailureInfo;

public interface FailureInfoService {

	void insert(FailureInfo fi);

	FailureInfo queryById(String cityCode, String deviceId);

	void update(FailureInfo fi);

	Integer deleteByDeviceId(String cityCode, String deviceId, String failureType);

	List<FailureInfo> queryList();

	Integer delete(FailureInfo fi);

	Map<String, Object> queryDeviceCountByCityCode(String city_code);

	List<FailureInfo> queryListByCityCode(String city_code);

	List<Map<String, Object>> queryFailureTypeCountByCityCode(String city_code);

	List<Map<String, Object>> queryStationDeviceCountByCityCode(String city_code);

	List<Map<String, Object>> queryDeviceStatusByStationName(String city_code, String station_name);
	
	/**
	 * 查询指定城市站点故障类型、设备类型的汇总
	 * @param city_code
	 * @return
	 */
	List<Map<String, Object>> queryStationDeviceTypeCountByCityCode(String city_code);

	/**
	 * 指定城市全部故障类型、设备类型的汇总
	 * @param city_code
	 * @return
	 */
	List<Map<String, Object>> queryFailureDeviceTypeCountByCityCode(
			String city_code);

	/**
	 * 站点、设备汇总
	 * @param city_code
	 * @return
	 */
	Map<String, Object> queryDeviceTypeCountByCityCode(String city_code);
	/**
	 * 设备离线跟其他故障重复数据汇总
	 * @return
	 */
	public Map<String, Object> queryOfflineAndOtherRepeatCount();
	List<Map<String, Object>> queryRtRecording(String city_code, 	String station_name, String device_id, 
			String device_type, String status_value, String start_time, String end_time, int offset);
	
	int countTotalNum(String city_code, 	String station_name, String device_id, 
			String device_type, String status_value, String start_time, String end_time);
	
}
