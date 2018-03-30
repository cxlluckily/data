package com.shankephone.data.monitoring.web.device.dao.mysql;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.web.device.model.FailureInfo;

/**
 * 故障信息Dao接口
 * @author fengql
 * @version 2017年10月11日 下午1:27:48
 */
public interface FailureInfoDao {

	public FailureInfo queryById(@Param("city_code")String cityCode, @Param("device_id")String deviceId);

	public void insert(FailureInfo fi);

	public void update(FailureInfo fi);

	public Integer deleteByDeviceId(@Param("city_code")String cityCode, 
			@Param("device_id")String deviceId, @Param("failure_type")String failureType);

	public List<FailureInfo> queryList();  
	
	public Map<String, Object> queryDeviceCountByCityCode(@Param("city_code")String city_code);

	public List<FailureInfo> queryListByCityCode(@Param("city_code")String city_code);

	public List<Map<String, Object>> queryFailureTypeCountByCityCode(@Param("city_code")String city_code);

	public List<Map<String, Object>> queryStationDeviceCountByCityCode(@Param("city_code")String city_code); 
	
	/**
	 * 指定站点名故障明细
	 * @param city_code
	 * @param station_name
	 * @return
	 */
	public List<Map<String, Object>> queryDeviceStatusByStationName(@Param("city_code")String city_code, @Param("station_name")String station_name);
	
	/**
	 * 指定城市站点故障类型汇总
	 * @param city_code
	 * @return
	 */
	public List<Map<String, Object>> queryStationDeviceTypeCountByCityCode(@Param("city_code")String city_code);

	/**
	 * 指定城市故障类型汇总
	 * @param city_code
	 * @return
	 */
	public List<Map<String, Object>>  queryFailureDeviceTypeCountByCityCode(@Param("city_code")String city_code);

	/**
	 * 站点、设备汇总
	 * @param city_code
	 * @return
	 */
	public Map<String, Object> queryDeviceTypeCountByCityCode(
			@Param("city_code")String city_code);
	/**
	 * 设备离线跟其他故障重复数据汇总
	 * @return
	 */
	public Map<String, Object> queryOfflineAndOtherRepeatCount();
	

	public List<Map<String, Object>> queryRtRecording(@Param("city_code")String city_code, 
			@Param("station_name")String station_name, @Param("device_id")String device_id, 
			@Param("device_type")String device_type, @Param("status_value")String status_value, 
			@Param("start_time")String startTime, @Param("end_time")String endTime, @Param("offset")int offset);
	
	public int countTotalNum(@Param("city_code")String city_code, 
			@Param("station_name")String station_name, @Param("device_id")String device_id, 
			@Param("device_type")String device_type, @Param("status_value")String status_value, 
			@Param("start_time")String startTime, @Param("end_time")String endTime);
}
