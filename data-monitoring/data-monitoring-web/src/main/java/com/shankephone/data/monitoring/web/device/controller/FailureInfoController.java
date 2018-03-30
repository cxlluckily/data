package com.shankephone.data.monitoring.web.device.controller;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.Device;
import com.shankephone.data.cache.DeviceCache;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.monitoring.util.Constants;
import com.shankephone.data.monitoring.web.device.service.FailureInfoHistoryService;
import com.shankephone.data.monitoring.web.device.service.FailureInfoService;

@Controller
@RequestMapping("/details")
public class FailureInfoController {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Resource
	private FailureInfoService failureInfoService;
	@RequestMapping("/station")
	@ResponseBody
	public Map<String,Object> station(HttpServletRequest request, HttpServletResponse response,String code)
			throws Exception {
		
		String cityCode = request.getParameter("cityCode");
		String stationCode = request.getParameter("stationCode");
		String stationName = request.getParameter("stationName");

		List<Map<String, Object>> devices = failureInfoService.queryDeviceStatusByStationName(cityCode, stationName);
		JSONObject resultJson = new JSONObject();
		JSONArray ticketMachine = new JSONArray();
		JSONArray gateMachine = new JSONArray();
		Set<String> failureDeviceSet = new HashSet<String>();
		//故障设备
		for(Map<String, Object> device : devices) {
			String deviceType = (String)device.get("device_type");
			String deviceId = (String)device.get("device_id");
			String deviceName = (String)device.get("device_name");
			String[] failureStatusCode = ((String)device.get("failure_status")).split(",");
			Date failureTime = (Date) device.get("failure_time");
			long duration = (Long)device.get("duration");
			String areaCodeZH = getAreaCodeZh((String)device.get("area_code"));
			JSONObject deviceStatus = new JSONObject();
			
			int failureTypeNum = failureStatusCode.length;
			if (failureTypeNum>1)
				failureStatusCode = getPriority(failureStatusCode);
			
			for (int i=0; i<failureTypeNum;i++)
				failureStatusCode[i] = Constants.failureTypeMap.get(failureStatusCode[i]);
			
			deviceStatus.put("deviceId", deviceId);
			deviceStatus.put("deviceName", deviceName);
			deviceStatus.put("failureType", failureStatusCode);
			deviceStatus.put("failureTime", sdf.format(failureTime));
			deviceStatus.put("duration", duration);
			deviceStatus.put("areaCode", areaCodeZH);
			
			if ( deviceType.equals("01") )
				ticketMachine.add(deviceStatus);	//售票机
			else
				gateMachine.add(deviceStatus);	//闸机
			
			failureDeviceSet.add(deviceId);
			
		}
		List<Device> allDevices = DeviceCache.getInstance().getByStationName(cityCode, stationName);
		String lineName = null;
		//正常设备
		for (Device device : allDevices){
			String deviceId = device.getDeviceId();
			lineName = device.getLineNameZh();
			if(!failureDeviceSet.contains(deviceId)){
				JSONObject deviceStatus = new JSONObject();
				String deviceType = device.getDeviceType();
				String deviceName = device.getDeviceName();
				String areaCodeZH = getAreaCodeZh(device.getAreaCode());
				
				deviceStatus.put("deviceId", deviceId);
				deviceStatus.put("deviceName", deviceName);
				deviceStatus.put("areaCode", areaCodeZH);
				
				if ( deviceType.equals("01") )
					ticketMachine.add(deviceStatus);
				else
					gateMachine.add(deviceStatus);
				
				failureDeviceSet.add(deviceId);
			}
		}
		
		resultJson.put("lineName", lineName);
		resultJson.put("stationName", stationName);
		resultJson.put("ticketMachine", ticketMachine);
		resultJson.put("gateMachine", gateMachine);

		return resultJson;
	}
	
	
	@RequestMapping("/rtRecording")
	@ResponseBody
	public JSONObject rtRecording(HttpServletRequest request, HttpServletResponse response,String code)
			throws Exception {
		String cityCode_param = request.getParameter("cityCode");
		String stationName_param = request.getParameter("stationName");
		String deviceId_param = request.getParameter("deviceId");
		String deviceType_param = request.getParameter("deviceType");
		String deviceTypeCode_param = null;
		String failureType_param = request.getParameter("failureType");
		String statusValue_param = null;
		String startTime = request.getParameter("startTime");
		String endTime = request.getParameter("endTime");
		String page = request.getParameter("page");
		int offset = (Integer.parseInt(page==null?"1":page)-1)*10;
		
		
		
		if ( deviceType_param!=null &&! deviceType_param.equals("") )
			deviceTypeCode_param = deviceType_param.equals("购票机")?"01":"02";
		for(String typeCode : Constants.failureTypeMap.keySet()){
			if (Constants.failureTypeMap.get(typeCode).equals(failureType_param))
				statusValue_param = typeCode;
		}
		
		int totalPages = 0;
		int totalNum = failureInfoService.countTotalNum(cityCode_param, stationName_param, 
				deviceId_param, deviceTypeCode_param, statusValue_param, startTime, endTime);
		totalPages = totalNum/10+(totalNum%10==0?0:1);
		
		List<Map<String, Object>> devices = failureInfoService.queryRtRecording(cityCode_param, stationName_param, 
				deviceId_param, deviceTypeCode_param, statusValue_param, startTime, endTime, offset);
		
		
		JSONObject result = new JSONObject();
		JSONArray data = new JSONArray();
		for (Map<String, Object> device : devices) {
			JSONObject record = new JSONObject();
			String stationName = (String) device.get("station_name");
			String deviceType = (String)device.get("device_type");
			String deviceId = (String)device.get("device_id");
			String failureType = Constants.failureTypeMap.get((String)device.get("status_value"));
			Date failureTime = (Date) device.get("failure_time");
			/*缺少故障原因*/
			long duration = (Long)device.get("duration");
			String areaCodeZH = getAreaCodeZh((String)device.get("area_code"));
			
			record.put("stationName", stationName);
			if (deviceType != null) {
				record.put("deviceType", deviceType.equals("01")?"购票机":"云闸机");
			} else {
				record.put("deviceType", deviceType);
			}
			record.put("deviceId", deviceId);
			record.put("failureType", failureType);
			record.put("failureTime", sdf.format(failureTime));
			record.put("duration", duration);
			record.put("areaCode", areaCodeZH);
			
			data.add(record);
		}
		result.put("data", data);
		result.put("totalPages", totalPages);
		return result;
	}
	
	/**
	 * 故障排行
	* @Title: getFailureRank 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param request
	* @param @param response
	* @param @return
	* @param @throws Exception    参数  
	* @return Object    返回类型  
	* @throws
	 */
	@RequestMapping("/getFailureRank")
	@ResponseBody
	public Object getFailureRank(HttpServletRequest request, HttpServletResponse response)throws Exception {
		String city_code = request.getParameter("cityCode");
		Calendar ca = Calendar.getInstance();
		ca.setTime(new Date()); 
		ca.add(Calendar.DAY_OF_MONTH, -7); 
		Date start_time = ca.getTime(); //结果
		
		FailureInfoHistoryService failureInfoHistoryService = (FailureInfoHistoryService)ContextAccessor.getBean("failureInfoHistoryService");
		List<Map<String, Object>> devices = failureInfoHistoryService.queryFailureRank(sdf.format(start_time), city_code);	
		
		JSONArray frequence_rank = new JSONArray();
		System.out.println("【hist】"+start_time);
		for(Map<String, Object> device : devices){
			System.out.println("【hist】"+device);
			String station_id = (String) device.get("station_code");
			String station_name = (String) device.get("station_name");
			String device_id = (String) device.get("device_id");
			long failure_times = (Long) device.get("failure_times");
			JSONObject device_details = new JSONObject();
			device_details.put("station_id", station_id);
			device_details.put("station_name", station_name);
			device_details.put("device_id",device_id );
			device_details.put("failure_times", failure_times);
			frequence_rank.add(device_details);
		}
		return frequence_rank;
	}
	
	
	/**
	 * 将"EXIT_A"转换为"A口"
	 * @author 森
	 * @date 2017年11月10日 下午3:45:52
	 * @param areaCode
	 * @return
	 */
	public static String getAreaCodeZh(String areaCode){
		if (areaCode==null||areaCode.length()==0||areaCode.split("_")[1].equals("X"))
			areaCode = "未知";
		else
			areaCode = areaCode.split("_")[1] +"口";
		return areaCode;
	}
	
	/**
	 * 将故障状态按优先级排序
	 * @author 森
	 * @date 2017年11月10日 下午3:46:55
	 * @param failureStatusCode
	 * @return
	 */
	public static String[] getPriority(String[] failureStatusCode){
		int[] priority = new int[failureStatusCode.length];
		for (int i=0; i<failureStatusCode.length; i++) {
			switch (failureStatusCode[i]) {
			case "9":
				priority[i] = 0;
				break;
			case "3":
				priority[i] = 1;
				break;
			case "4":
				priority[i] = 2;
				break;
			case "2":
				priority[i] = 3;
				break;
			case "1":
				priority[i] = 4;
				break;		
			default:
				break;
			}
		}
		Arrays.sort(priority);
		for (int i=0; i<priority.length; i++) {
			switch (priority[i]) {
			case 0:
				failureStatusCode[i] = "9";
				break;
			case 1:
				failureStatusCode[i] = "3";
				break;
			case 2:
				failureStatusCode[i] = "4";
				break;
			case 3:
				failureStatusCode[i] = "2";
				break;
			case 4:
				failureStatusCode[i] = "1";
				break;		
			default:
				break;
			}
		}
		return failureStatusCode;
	}

}
