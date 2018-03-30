package com.shankephone.data.monitoring.web.device.dao.mysql;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.web.device.model.FailureDetails;

/**
 * 故障信息Dao接口
 * @author fengql
 * @version 2017年10月11日 下午1:27:48
 */
public interface FailureDetailsDao {

	public void insert(FailureDetails fd);

	public void deleteByDeviceId(@Param("city_code")String cityCode, @Param("device_id")String deviceId); 
	
	public void deleteByDeviceAndStatus(@Param("city_code")String cityCode, @Param("device_id")String deviceId,@Param("status_id")String statusId); 

}
