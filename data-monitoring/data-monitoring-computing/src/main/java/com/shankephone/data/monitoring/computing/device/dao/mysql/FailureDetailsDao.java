package com.shankephone.data.monitoring.computing.device.dao.mysql;

import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureDetails;

/**
 * 故障信息Dao接口
 * @author fengql
 * @version 2017年10月11日 下午1:27:48
 */
public interface FailureDetailsDao {

	public void insert(FailureDetails fd);

	public void deleteByDeviceId(@Param("city_code")String cityCode, @Param("device_id")String deviceId); 
	
	public void deleteByDeviceAndStatus(@Param("city_code")String cityCode, @Param("device_id")String deviceId,@Param("status_id")String statusId);

	public Map<String,Object> selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id, @Param("status_id")String status_id); 

	
}
