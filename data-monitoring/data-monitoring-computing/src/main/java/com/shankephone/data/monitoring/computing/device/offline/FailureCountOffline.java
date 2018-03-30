package com.shankephone.data.monitoring.computing.device.offline;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.Device;
import com.shankephone.data.cache.DeviceCache;
import com.shankephone.data.cache.DeviceStatusDic;
import com.shankephone.data.cache.DeviceStatusDicCache;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.monitoring.computing.device.model.FailureDetails;
import com.shankephone.data.monitoring.computing.device.model.FailureDetailsHistory;
import com.shankephone.data.monitoring.computing.device.model.FailureInfo;
import com.shankephone.data.monitoring.computing.device.model.FailureInfoHistory;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsHistoryService;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoHistoryService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoPhoenixService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoService;
import com.shankephone.data.monitoring.computing.util.Constants;

public class FailureCountOffline {
	private static final Logger logger = LoggerFactory.getLogger(FailureCountOffline.class);
	
	/**
	 * 故障高发排行
	 * @author 森
	 * @date 2017年10月31日 下午2:53:01
	 * @param start_time
	 */
	public void countFrequence(String start_time){
		FailureInfoPhoenixService failureInfoPhoenixService = (FailureInfoPhoenixService)ContextAccessor.getBean("failureInfoPhoenixService");
		List<Map<String, Object>> devices = failureInfoPhoenixService.queryFailureFrequence(start_time);		
		Map<String, JSONArray> result = new HashMap<String, JSONArray>();
		
		for(Map<String, Object> device : devices){
			String city_code = (String) device.get("CITY_CODE");
			String station_id = (String) device.get("STATION_CODE");
			String station_name = (String) device.get("STATION_NAME");
			String device_id = (String) device.get("DEVICE_ID");
			long failure_times = (long) device.get("FAILURE_TIMES");
			JSONArray frequence_rank = result.getOrDefault(city_code, new JSONArray());
			if(frequence_rank.size()==10)   //只取前十
				continue;
			JSONObject device_details = new JSONObject();
			device_details.put("station_id", station_id);
			device_details.put("station_name", station_name);
			device_details.put("device_id",device_id );
			device_details.put("failure_times", failure_times);
			frequence_rank.add(device_details);
			result.put(city_code, frequence_rank);
		}
		for(String city_code : result.keySet()){
			System.out.println("【toJSONString】"+result.get(city_code).toJSONString());
			RedisUtils.getRedissonClient()
							.getTopic("device_failure_rank_"+city_code)
							.publish(result.get(city_code).toJSONString());
		}
	}
	
	/**
	 * 从原生device_status表中获得历史故障设备 
	 * 补充hbase中的FAILURE_INFO和FAILURE_DETAILS表
	 * 并将当前故障设备写入mysql
	 * @author 森
	 * @date 2017年10月26日 下午3:54:28
	 * @param builder
	 * @param select_city_code
	 * @param starttime   yyyy-MM-dd HH:mm:ss
	 * @param endtime
	 */
	public void makeupHistFailure(SparkSQLBuilder builder, String select_city_code, String starttime, String endtime){
		SimpleDateFormat sdf_1 = new SimpleDateFormat("yyyyMMddHHmmss");  
		SimpleDateFormat sdf_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		
		try {		//将 yyyy-MM-dd HH:mm:ss转换为yyyyMMddHHmmss
			starttime = sdf_1.format(sdf_2.parse(starttime));
			endtime = sdf_1.format(sdf_2.parse(endtime));
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		Map<String, Object> sqlParams = new HashMap<>();
		if (select_city_code!=null)
			sqlParams.put("select_city_code", "'"+select_city_code+"'");
		if (starttime!=null)
			sqlParams.put("starttime", "'"+starttime+"'");
		if (endtime!=null)
			sqlParams.put("endtime", "'"+endtime+"'");

		Dataset<Row> sub_device_status = builder.executeSQL("sub_device_status", sqlParams);		
		sub_device_status.createOrReplaceTempView("sub_device_status");
		Dataset<Row> rows = builder.executeSQL("device_failure", null);

		rows.foreachPartition(func->{
			FailureInfoHistoryService failureInfoHistoryService = (FailureInfoHistoryService)ContextAccessor.getBean("failureInfoHistoryService");
			FailureDetailsHistoryService failureDetailsHistoryService = (FailureDetailsHistoryService)ContextAccessor.getBean("failureDetailsHistoryService");
			FailureInfoService failureInfoMysqlService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
			FailureDetailsService failureDetailsMysqlService = (FailureDetailsService)ContextAccessor.getBean("failureDetailsService");
			logger.info("-----------------【SQL finish】----------------");
			while(func.hasNext()){
				Row device = func.next();
				String city_code = device.getString(0);
				String device_id = device.getString(1);
				String status_id = device.getString(2);
				String status_value = device.getString(3);
				Date device_time = null;
				String device_time_str = device.getString(4);
				Date update_date_pre = null;
				String update_date_pre_str = device.getString(5);
				String failure_time_str = device.getString(6);
				Date failure_time = null;
				String recover_time_str = device.getString(7);
				Date recover_time = null;
				
				String failure_type = Constants.FAILURE_TYPE_FAILURE;
				Device deviceCache = DeviceCache.getInstance().get(city_code, device_id);
				DeviceStatusDic deviceStatusDicCache = DeviceStatusDicCache.getInstance().get(status_id, status_value);

				//将 字典表无故障信息、STATUS_TYPE为非故障的数据 忽略
				if (deviceStatusDicCache==null || deviceStatusDicCache.getStatusType().equals("0"))
					continue;
				
				try {
					device_time = device_time_str==null?null:sdf_1.parse(device_time_str);
					failure_time = failure_time_str==null?null:sdf_1.parse(failure_time_str);
					recover_time = recover_time_str==null?null:sdf_1.parse(recover_time_str);
					update_date_pre = update_date_pre_str==null?null:sdf_1.parse(update_date_pre_str);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				Date create_time = new Date();
				
				String city_name = Constants.cityMap.get(city_code);
				String line_code = null;
				String line_name = 	null;
				String station_code = null;
				String station_name = null;
				String device_name = null;
				String device_type = null;
				String device_type_name = null;
				String area_code = null;
				if (deviceCache==null){
					logger.warn("【设备在redis中不存在】-----city_code : " +city_code+"  device_id :"+device_id);
					line_code = device_id.substring(0, 2);
					station_code = device_id.substring(0, 4);
					device_type = device_id.substring(4, 6).equals("02")?"01":"02";
					device_type_name = device_type.equals("01")?"购票机":"闸机";
				}
					
				else {
					line_code = deviceCache.getLineCode();
					line_name = deviceCache.getLineNameZh();
					station_code = deviceCache.getStationCode();
					station_name = deviceCache.getStationNameZh();
					device_name = deviceCache.getDeviceName();
					device_type = deviceCache.getDeviceType();
					
					if (device_type==null||device_type.isEmpty())
						logger.info("【设备类型不存在】-----city_code : " +city_code+"  device_id :"+device_id);
					else
						device_type_name = device_type.equals("01")?"购票机":"闸机";
					
					area_code = deviceCache.getAreaCode();
				}
				
				String reason = deviceStatusDicCache.getStatusIdName()+" - "+deviceStatusDicCache.getStatusValueName();
					
				if (status_id.equals("0000")) {
					FailureInfoHistory infoPh = new FailureInfoHistory();
					infoPh.setCity_code(city_code);
					infoPh.setCity_name(city_name);
					infoPh.setLine_code(line_code);
					infoPh.setLine_name(line_name);
					infoPh.setStation_code(station_code);
					infoPh.setStation_name(station_name);
					infoPh.setDevice_id(device_id);
					infoPh.setDevice_name(device_name);
					infoPh.setDevice_type(device_type);
					infoPh.setDevice_type_name(device_type_name);
					infoPh.setFailure_type(failure_type);
					infoPh.setStatus_value(status_value);
					infoPh.setArea_code(area_code);
					infoPh.setRecover_time(recover_time);
					infoPh.setCreate_time(create_time);
					
					if(failure_time==null){  //查找原表中是否有故障时间
						Map<String, Object> PK = failureInfoHistoryService.selectPK(city_code, device_id, failure_type);
						if (PK==null){		//若原表中没有故障时间
							failure_time= update_date_pre ==null?device_time:update_date_pre;
						}
						else
							failure_time = (Date) PK.get("failure_time");
					}
					infoPh.setFailure_time(failure_time);					
					failureInfoHistoryService.insert(infoPh);//将故障数据存入mysql的failure_info_history表

					if(recover_time==null){     
						FailureInfo infoMy = new FailureInfo();
						infoMy.setCity_code(city_code);
						infoMy.setCity_name(city_name);
						infoMy.setLine_code(line_code);
						infoMy.setLine_name(line_name);
						infoMy.setStation_code(station_code);
						infoMy.setStation_name(station_name);
						infoMy.setDevice_id(device_id);
						infoMy.setDevice_name(device_name);
						infoMy.setDevice_type(device_type);
						infoMy.setDevice_type_name(device_type_name);
						infoMy.setFailure_type(failure_type);
						infoMy.setStatus_value(status_value);
						infoMy.setArea_code(area_code);
						infoMy.setFailure_time(failure_time);
						infoMy.setCreate_time(create_time);
						failureInfoMysqlService.insert(infoMy);//将当前故障数据存入mysql的failure_info表
					}				
				}
				else {
					FailureDetailsHistory detailsPh = new FailureDetailsHistory();
					detailsPh.setCity_code(city_code);
					detailsPh.setCity_name(city_name);
					detailsPh.setLine_code(line_code);
					detailsPh.setLine_name(line_name);
					detailsPh.setStation_code(station_code);
					detailsPh.setStation_name(station_name);
					detailsPh.setDevice_id(device_id);
					detailsPh.setDevice_name(device_name);
					detailsPh.setDevice_type(device_type);
					detailsPh.setDevice_type_name(device_type_name);
					detailsPh.setStatus_id(status_id);
					detailsPh.setStatus_value(status_value);
					detailsPh.setReason(reason);
					detailsPh.setFailure_time(failure_time);
					detailsPh.setRecover_time(recover_time);
					detailsPh.setCreate_time(create_time);
					
					if(failure_time==null){  //查找原表中是否有故障时间
						List<Map<String, Object>> PKList = failureDetailsHistoryService.selectPK(city_code, device_id, status_id);
						if (PKList==null){
							failure_time = update_date_pre ==null?device_time:update_date_pre;
						}
						else
							failure_time = (Date) PKList.get(0).get("failure_time");
					}
					detailsPh.setFailure_time(failure_time);
					
					failureDetailsHistoryService.insert(detailsPh);//将故障数据存入mysql的failure_details_history表
					
					if(recover_time==null){    
						FailureDetails detailsMy = new FailureDetails();
						detailsMy.setCity_code(city_code);
						detailsMy.setCity_name(city_name);
						detailsMy.setLine_code(line_code);
						detailsMy.setLine_name(line_name);
						detailsMy.setStation_code(station_code);
						detailsMy.setStation_name(station_name);
						detailsMy.setDevice_id(device_id);
						detailsMy.setDevice_name(device_name);
						detailsMy.setDevice_type(device_type);
						detailsMy.setDevice_type_name(device_type_name);
						detailsMy.setStatus_id(status_id);
						detailsMy.setStatus_value(status_value);
						detailsMy.setReason(reason);
						detailsMy.setFailure_time(failure_time);
						detailsMy.setCreate_time(create_time);
						failureDetailsMysqlService.insert(detailsMy);//将当前故障数据存入mysql的failure_details表
					}
				}
			}
		});
		
	}
	
	/**
	 * 设备历史离线数
	 * @author 森
	 * @date 2017年10月31日 上午10:26:15
	 * @param builder
	 * @param select_city_code
	 * @param starttime yyyy-MM-dd HH:mm:ss
	 * @param endtime
	 */
	public void makeupHistOffline(SparkSQLBuilder builder, String select_city_code, String starttime, String endtime){
		SimpleDateFormat sdf_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		Map<String, Object> sqlParams = new HashMap<>();
		if (select_city_code!=null)
			sqlParams.put("select_city_code", "'"+select_city_code+"'");
		if (starttime!=null)
			sqlParams.put("starttime", "'"+starttime+"'");
		if (endtime!=null)
			sqlParams.put("endtime", "'"+endtime+"'");
		
		sqlParams.put("TTL_SECOND", PropertyAccessor.getProperty("heartBeat.TTL_SECOND"));
		sqlParams.put("RECOVER_TIMES", PropertyAccessor.getProperty("heartBeat.RECOVER_TIMES"));

		Dataset<Row> device_offline = builder.executeSQL("device_offline", sqlParams);	
		
		device_offline.foreachPartition(func->{
			FailureInfoHistoryService failureInfoService = (FailureInfoHistoryService)ContextAccessor.getBean("failureInfoHistoryService");
			while (func.hasNext()) {
				Row row = func.next();
				String city_code = row.getString(0);
				String device_id = row.getString(1);
				String failure_time_str = row.getString(2);
				String recover_time_str = row.getString(3);
				String failure_type = Constants.FAILURE_TYPE_OFFLINE;
				String status_value = Constants.FAILURE_STATUS_OFFLINE;
				
				Date create_time = new Date();
				
				Device deviceCache = DeviceCache.getInstance().get(city_code, device_id);
				String city_name = Constants.cityMap.get(city_code);
				String line_code = null;
				String line_name = 	null;
				String station_code = null;
				String station_name = null;
				String device_name = null;
				String device_type = null;
				String device_type_name = null;
				String area_code = null;
				if (deviceCache==null){
					logger.warn("【设备不存在】-----city_code : " +city_code+"  device_id :"+device_id);
					line_code = device_id.substring(0, 2);
					station_code = device_id.substring(0, 4);
					device_type = device_id.substring(4, 6).equals("02")?"01":"02";
					device_type_name = device_type.equals("01")?"购票机":"闸机";
				}
				else {
					line_code = deviceCache.getLineCode();
					line_name = deviceCache.getLineNameZh();
					station_code = deviceCache.getStationCode();
					station_name = deviceCache.getStationNameZh();
					device_name = deviceCache.getDeviceName();
					device_type = deviceCache.getDeviceType();
					
					if (device_type==null||device_type.isEmpty())
						logger.info("【设备类型不存在】-----city_code : " +city_code+"  device_id :"+device_id);
					else
						device_type_name = device_type.equals("01")?"购票机":"闸机";
					
					area_code = deviceCache.getAreaCode();
				}

				FailureInfoHistory infoPh = new FailureInfoHistory();
				infoPh.setCity_code(city_code);
				infoPh.setCity_name(city_name);
				infoPh.setLine_code(line_code);
				infoPh.setLine_name(line_name);
				infoPh.setStation_code(station_code);
				infoPh.setStation_name(station_name);
				infoPh.setDevice_id(device_id);
				infoPh.setDevice_name(device_name);
				infoPh.setDevice_type(device_type);
				infoPh.setDevice_type_name(device_type_name);
				infoPh.setFailure_type(failure_type);
				infoPh.setStatus_value(status_value);
				infoPh.setArea_code(area_code);
				infoPh.setFailure_time(failure_time_str==null?null:sdf_2.parse(failure_time_str));
				infoPh.setRecover_time(recover_time_str==null?null:sdf_2.parse(recover_time_str));
				infoPh.setCreate_time(create_time);
				failureInfoService.insert(infoPh);
			}
		});
	}
	

}
