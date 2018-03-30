package com.shankephone.data.monitoring.computing.device.dao.phoenix;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.shankephone.data.monitoring.computing.device.model.FailureDetailsPhoenix;

public interface FailureDetailsPhoenixDao {

	public List<FailureDetailsPhoenix> select();
	
	public void insert(FailureDetailsPhoenix failInfo);
	
	public void deleteByPK(@Param("PK")String PK);
	
	public String selectPK(@Param("city_code")String city_code, @Param("device_id")String device_id, @Param("status_id")String status_id);

}
