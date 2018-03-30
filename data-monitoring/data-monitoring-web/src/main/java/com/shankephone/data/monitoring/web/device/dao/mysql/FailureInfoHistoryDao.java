package com.shankephone.data.monitoring.web.device.dao.mysql;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface FailureInfoHistoryDao {
	public List<Map<String, Object>> queryFailureRank(@Param("start_time")String start_time, @Param("city_code")String city_code);

}
