package com.shankephone.data.monitoring.web.device.service;

import java.util.List;
import java.util.Map;

public interface FailureInfoHistoryService {
	public List<Map<String, Object>> queryFailureRank(String start_time, String city_code);
}
