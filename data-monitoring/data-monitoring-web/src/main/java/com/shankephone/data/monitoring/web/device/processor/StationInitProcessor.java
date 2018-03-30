package com.shankephone.data.monitoring.web.device.processor;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.monitoring.util.Constants;
import com.shankephone.data.monitoring.web.device.service.FailureInfoService;
import com.shankephone.data.monitoring.web.device.service.impl.FailureInfoServiceImpl;

public class StationInitProcessor implements SubDataInitProcessor {
	
	private final static String topicPrefix = "device_failure_station_";
	
	
	@Override
	public Object process(SubInfo subInfo) {
		return failureDeviceTypeTotalProcess(subInfo);
	}

	/**
	 * 按故障类型、设备类型汇总
	 * @param subInfo
	 * @return
	 */
	private Object failureDeviceTypeTotalProcess(SubInfo subInfo) {
		String topics = subInfo.getTopic();
		String city_code = topics.replace(topicPrefix, "");
		FailureInfoService failureInfoService = (FailureInfoServiceImpl)ContextAccessor.getBean("failureInfoService");
		
		JSONObject result = new JSONObject();
		//站点和设备故障数量汇总
		Map<String,Object> deviceCountMap = failureInfoService.queryDeviceTypeCountByCityCode(city_code);
		//故障类型数量汇总
		List<Map<String,Object>> failureTypeCountMap = failureInfoService.queryFailureDeviceTypeCountByCityCode(city_code);
		//站点、故障状态数量
		List<Map<String,Object>> stationDeviceCountMap = failureInfoService.queryStationDeviceTypeCountByCityCode(city_code);
		//设备离线跟其他故障重复数据汇总
		Map<String,Object> mapFalg=failureInfoService.queryOfflineAndOtherRepeatCount();
		if(deviceCountMap==null){
			return result;
		}
		//设备汇总
		BigDecimal station_num = deviceCountMap.get("station_num")==null?new BigDecimal("0"):(BigDecimal)deviceCountMap.get("station_num");
		BigDecimal device_num = deviceCountMap.get("device_num")==null?new BigDecimal("0"):(BigDecimal)deviceCountMap.get("device_num");
		result.put("failure_station_num", station_num);
		result.put("failure_device_num", device_num);
		
		if(failureTypeCountMap==null){
			return result;
		}
		//各类型汇总
		for(Map<String, Object> typeMap : failureTypeCountMap){
			String status_value = (String)typeMap.get("status_value");
			BigDecimal ticket_num = (BigDecimal)typeMap.get("ticket_num");
			BigDecimal gate_num = (BigDecimal)typeMap.get("gate_num");
			JSONObject json = new JSONObject();
			json.put("ticket_num", ticket_num.longValue());
			json.put("gate_num", gate_num.longValue());
			json.put("total_num", ticket_num.longValue() + gate_num.longValue());
			if(Constants.FAILURE_MODE_STOP.equals(status_value)){
				result.put("device_stop_num", json);
			}
			if(Constants.FAILURE_MODE_REPAIR.equals(status_value)){
				result.put("device_repair_num", json);
			}
			if(Constants.FAILURE_MODE_FAILURE.equals(status_value)){
				result.put("device_failure_num", json);
			}
			if(Constants.FAILURE_MODE_INTERRUPT.equals(status_value)){
				result.put("device_interrupt_num", json);
			}
			if(Constants.FAILURE_STATUS_OFFLINE.equals(status_value)){
				json.put("GPJ_stopRepeat", mapFalg==null || mapFalg.get("stop_gpj")==null?0:mapFalg.get("stop_gpj"));
				json.put("GPJ_repairRepeat", mapFalg==null || mapFalg.get("repair_gpj")==null?0:mapFalg.get("repair_gpj"));
				json.put("GPJ_failureRepeat", mapFalg==null || mapFalg.get("failure_gpj")==null?0:mapFalg.get("failure_gpj"));
				json.put("GPJ_interruptRepeat", mapFalg==null || mapFalg.get("interrupt_gpj")==null?0:mapFalg.get("interrupt_gpj"));
				json.put("ZJ_stopRepeat", mapFalg==null || mapFalg.get("stop_zj")==null?0:mapFalg.get("stop_zj"));
				json.put("ZJ_repairRepeat", mapFalg==null || mapFalg.get("repair_zj")==null?0:mapFalg.get("repair_zj"));
				json.put("ZJ_failureRepeat", mapFalg==null || mapFalg.get("failure_zj")==null?0:mapFalg.get("failure_zj"));
				json.put("ZJ_interruptRepeat", mapFalg==null || mapFalg.get("interrupt_zj")==null?0:mapFalg.get("interrupt_zj"));
				
				result.put("device_offline_num", json);
			}
		}
		if(stationDeviceCountMap==null){
			return result;
		}
		//各站点故障类型汇总
		Map<String, JSONObject> stationMap = new HashMap<String, JSONObject>();
		for(Map<String,Object> m : stationDeviceCountMap){
			String station_code = (String)m.get("station_code");
			String status_value = (String)m.get("status_value");
			String station_name = (String)m.get("station_name");
			BigDecimal ticket_num = (BigDecimal)m.get("ticket_num");
			BigDecimal gate_num = (BigDecimal)m.get("gate_num");
			JSONObject numj = new JSONObject();
			numj.put("ticket_num", ticket_num.longValue());
			numj.put("gate_num", gate_num.longValue());
			numj.put("total_num", ticket_num.longValue() + gate_num.longValue());
			JSONObject j = stationMap.get(station_name);
			if(j == null){
				j = new JSONObject();
				j.put("station_id", station_code);
				j.put("station_name", station_name);
			}
			
			if(Constants.FAILURE_MODE_STOP.equals(status_value)){
				j.put("device_stop_num", numj);
			}
			if(Constants.FAILURE_MODE_REPAIR.equals(status_value)){
				j.put("device_repair_num", numj);
			}
			if(Constants.FAILURE_MODE_FAILURE.equals(status_value)){
				j.put("device_failure_num", numj);
			}
			if(Constants.FAILURE_MODE_INTERRUPT.equals(status_value)){
				j.put("device_interrupt_num", numj);
			}
			if(Constants.FAILURE_STATUS_OFFLINE.equals(status_value)){
				j.put("device_offline_num", numj);
			}
			stationMap.put(station_name, j);
		}
		
		//各站点故障数量
		JSONArray array = new JSONArray();
		for(String station_code : stationMap.keySet()){
			JSONObject j = stationMap.get(station_code);
			array.add(j);
		}	
		result.put("station", array);
		System.out.println(result.toJSONString()); 
		return result;
	}
	
	private Object failureTotalProcess(SubInfo subInfo) {
		String topics = subInfo.getTopic();
		String city_code = topics.replace(topicPrefix, "");
		FailureInfoService failureInfoService = (FailureInfoServiceImpl)ContextAccessor.getBean("failureInfoService");
		
		JSONObject result = new JSONObject();
		//站点和设备故障数量汇总
		Map<String,Object> deviceCountMap = failureInfoService.queryDeviceCountByCityCode(city_code);
		//故障类型数量汇总
		List<Map<String,Object>> failureTypeCountMap = failureInfoService.queryFailureTypeCountByCityCode(city_code);
		//站点、故障状态数量
		List<Map<String,Object>> stationDeviceCountMap = failureInfoService.queryStationDeviceCountByCityCode(city_code);
		
		//设备汇总
		Long station_num = (Long)deviceCountMap.get("station_num");
		BigDecimal device_num = (BigDecimal)deviceCountMap.get("device_num");
	
		result.put("failure_station_num", station_num);
		result.put("failure_device_num", device_num);
		
		//各类型汇总
		for(Map<String, Object> typeMap : failureTypeCountMap){
			String status_value = (String)typeMap.get("status_value");
			long count = (Long)typeMap.get("device_num");
			if(Constants.FAILURE_MODE_STOP.equals(status_value)){
				result.put("device_stop_num", count);
			}
			if(Constants.FAILURE_MODE_REPAIR.equals(status_value)){
				result.put("device_repair_num", count);
			}
			if(Constants.FAILURE_MODE_FAILURE.equals(status_value)){
				result.put("device_failure_num", count);
			}
			if(Constants.FAILURE_MODE_INTERRUPT.equals(status_value)){
				result.put("device_interrupt_num", count);
			}
			if(Constants.FAILURE_STATUS_OFFLINE.equals(status_value)){
				result.put("device_offline_num", count);
			}
		}
		
		//各站点故障类型汇总
		Map<String, JSONObject> stationMap = new HashMap<String, JSONObject>();
		for(Map<String,Object> m : stationDeviceCountMap){
			String station_code = (String)m.get("station_code");
			String status_value = (String)m.get("status_value");
			String station_name = (String)m.get("station_name");
			long count = (Long)m.get("device_num");
			JSONObject j = stationMap.get(station_code);
			if(j == null){
				j = new JSONObject();
				j.put("station_id", station_code);
				j.put("station_name", station_name);
			}
			
			if(Constants.FAILURE_MODE_STOP.equals(status_value)){
				j.put("device_stop_num", count);
			}
			if(Constants.FAILURE_MODE_REPAIR.equals(status_value)){
				j.put("device_repair_num", count);
			}
			if(Constants.FAILURE_MODE_FAILURE.equals(status_value)){
				j.put("device_failure_num", count);
			}
			if(Constants.FAILURE_MODE_INTERRUPT.equals(status_value)){
				j.put("device_interrupt_num", count);
			}
			if(Constants.FAILURE_STATUS_OFFLINE.equals(status_value)){
				j.put("device_offline_num", count);
			}
			stationMap.put(station_code, j);
		}
		
		//各站点故障数量
		JSONArray array = new JSONArray();
		for(String station_code : stationMap.keySet()){
			JSONObject j = stationMap.get(station_code);
			array.add(j);
		}	
		result.put("station", array);
		return result;
	}
	
	

}
