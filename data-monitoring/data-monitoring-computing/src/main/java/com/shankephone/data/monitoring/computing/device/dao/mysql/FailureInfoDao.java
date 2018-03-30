package com.shankephone.data.monitoring.computing.device.dao.mysql;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureInfo;

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
	
	public List<Map<String,Object>> queryDeviceCount();
	
	public List<Map<String,Object>> queryFailureTypeCount();

	public List<Map<String, Object>> queryStationDeviceCount();

	public Map<String,Object> selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id,
			@Param("failure_type")String failure_type);

	/**
	 * 站点设备汇总
	 * @return
	 */
	public List<Map<String, Object>> queryDeviceTypeCount();

	/**
	 * 故障分类汇总
	 * @return
	 */
	public List<Map<String, Object>> queryFailureDeviceTypeCount();

	/**
	 * 站点故障汇总
	 * @return
	 */
	public List<Map<String, Object>> queryStationDeviceTypeCount(); 
	/**
	 * 设备离线跟其他故障重复数据汇总
	 * @return
	 */
	public Map<String, Object> queryOfflineAndOtherRepeatCount(); 
	
	public List<Map<String, Object>> queryFailureData(@Param("city_code")String city_code,@Param("status_value")String status_value);
}
