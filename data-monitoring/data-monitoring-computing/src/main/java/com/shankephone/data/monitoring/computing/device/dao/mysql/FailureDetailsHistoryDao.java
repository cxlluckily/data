package com.shankephone.data.monitoring.computing.device.dao.mysql;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureDetailsHistory;

/**
 * 故障信息Dao接口
 * @author fengql
 * @version 2017年10月11日 下午1:27:48
 */
public interface FailureDetailsHistoryDao {

	void insert(FailureDetailsHistory fdp);

	void deleteHistoryDetail(FailureDetailsHistory fdp); 

	public List<Map<String,Object>> selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id, @Param("status_id")String status_id);

	void recoverFailure(FailureDetailsHistory fdp);

	void updateStatus(FailureDetailsHistory fdp); 
	
}
