package com.shankephone.data.monitoring.computing.device.dao.mysql;

import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureInfoHistory;

/**
 * 故障信息Dao接口
 * @author fengql
 * @version 2017年10月11日 下午1:27:48
 */
public interface FailureInfoHistoryDao {

	void insert(FailureInfoHistory failInfo);

	void deleteByDeviceId(FailureInfoHistory failInfo);

	Map<String, Object> selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id,
			@Param("failure_type")String failure_type);

	void recoverFailure(FailureInfoHistory failInfo);

	void updateStatus(FailureInfoHistory failInfo);
	
}
