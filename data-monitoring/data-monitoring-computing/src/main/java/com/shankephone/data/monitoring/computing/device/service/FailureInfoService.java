package com.shankephone.data.monitoring.computing.device.service;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureInfo;

public interface FailureInfoService {

	void insert(FailureInfo fi);

	FailureInfo queryById(String cityCode, String deviceId);

	void update(FailureInfo fi);

	Integer deleteByDeviceId(String cityCode, String deviceId, String failureType);

	List<FailureInfo> queryList();

	List<Map<String, Object>> queryDeviceCount();

	Integer delete(FailureInfo fi);

	List<Map<String, Object>> queryFailureTypeCount();

	List<Map<String, Object>> queryStationDeviceCount();

	Map<String,Object> selectPK(String city_code, String device_id, String failure_type);

	/**
	 * 站点、设备汇总
	 * @return
	 */
	List<Map<String, Object>> queryDeviceTypeCount();

	/**
	 * 故障分类汇总
	 * @return
	 */
	List<Map<String, Object>> queryFailureDeviceTypeCount();

	/**
	 * 站点故障汇总
	 * @return
	 */
	List<Map<String, Object>> queryStationDeviceTypeCount();
	/**
	 * 设备离线跟其他故障重复数据汇总
	 * @return
	 */
	public Map<String, Object> queryOfflineAndOtherRepeatCount(); 
	/**
	 * 根据城市或状态值查询故障设备信息
	* @Title: queryFailureData 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param city_code
	* @param @param status_value
	* @param @return    参数  
	* @return List<Map<String,Object>>    返回类型  
	* @throws
	 */
	public List<Map<String, Object>> queryFailureData(String city_code,String status_value);
}
