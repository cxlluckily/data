package com.shankephone.data.monitoring.web.device.dao.phoenix;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.web.device.model.FailureInfoPhoenix;


public interface FailureInfoPhoenixDao {
	
	public List<FailureInfoPhoenix> select();
	
	public void insert(FailureInfoPhoenix failInfo);
	
	public void deleteByPK(@Param("PK")String PK);
	
	public String selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id, @Param("failure_type")String failure_type);

	public List<Map<String, Object>> queryFailureFrequence(@Param("start_time")String start_time, @Param("city_code")String city_code);
}
