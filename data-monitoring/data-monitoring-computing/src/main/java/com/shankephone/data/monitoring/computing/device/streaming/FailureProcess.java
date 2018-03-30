package com.shankephone.data.monitoring.computing.device.streaming;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import scala.Tuple2;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.City;
import com.shankephone.data.cache.CityCache;
import com.shankephone.data.cache.Device;
import com.shankephone.data.cache.DeviceCache;
import com.shankephone.data.cache.DeviceStatusDic;
import com.shankephone.data.cache.DeviceStatusDicCache;
import com.shankephone.data.cache.Station;
import com.shankephone.data.cache.StationCache;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.monitoring.computing.device.model.FailureDetails;
import com.shankephone.data.monitoring.computing.device.model.FailureDetailsHistory;
import com.shankephone.data.monitoring.computing.device.model.FailureInfo;
import com.shankephone.data.monitoring.computing.device.model.FailureInfoHistory;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsHistoryService;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoHistoryService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoService;
import com.shankephone.data.monitoring.computing.util.Constants;

/**
 * 设备故障实时处理
 * @author fengql
 * @version 2017年11月13日 上午9:32:08
 */
public class FailureProcess implements SparkStreamingProcessor {
	
	private static boolean debug = false;

	private final static Logger logger = LoggerFactory
			.getLogger(FailureProcess.class);
	
	public static void main(String[] args) {
		FailureProcess process = new FailureProcess();
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("startTime", "2018-03-27 17:30:00");
//		process.process(map);
//		process.initRedisKey();
//		compareDeviceAndHeart();
	}
	
	@Override
	public void beforeProcess(JavaStreamingContext context, Map<String, Object> args) {
		//心跳处理
		deviceHeartBeatProcess();
	}
	
	@Override
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd,Map<String, Object> argsMap) {
		
		JavaRDD<JSONObject> results = rdd.filter(f -> {
			String startTime = (String) argsMap.get("startTime");
			if(startTime==null || "".equals(startTime)){
				startTime = DateUtils.formatDateTime(new Date());
			}
			return filterData(f, startTime);
		}).map(r -> {
			return r;
		});
		//分组
		JavaPairRDD<String, Iterable<JSONObject>> pairRdd = groupByKey(results);
		//计算处理
		process(pairRdd);
		//推送数据
		publishData();
	}

	private void deviceHeartBeatProcess() {
		//清除redisKey
		clearExpireKey();
		//初始化redisKey
		initRedisKey();
		//监控过期key
		listenHeartBeat();
	}

	//清除所有心跳缓存
	private void clearExpireKey() {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RKeys keys = redisson.getKeys();
		keys.deleteByPattern("heartbeat:*");
	}

	public void initRedisKey() {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		Map<String,Device> deviceCacheMap=DeviceCache.getInstance().getCacheMap();
		for (String key : deviceCacheMap.keySet()){
			Device bean = deviceCacheMap.get(key);
			String device_id = bean.getDeviceId();
			String city_code = bean.getCityCode();
			if(city_code != null && !"".equals(city_code)){
				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				RBucket<JSONObject> bucket = redisson.getBucket("heartbeat:"+city_code+":"+device_id);
				JSONObject bk = new JSONObject();
				bk.put("timestamp", sdf.format(date)); 
				bk.put("count", 0);
				bucket.set(bk);
				bucket.expire(Constants.HEART_EXPIRE_MINUTES, TimeUnit.MINUTES);
			}
		}
	}
	

	/**
	 * 故障数据分组处理
	 * @param sdf
	 * @param sdft
	 * @param results
	 * @return
	 */
	private JavaPairRDD<String, Iterable<JSONObject>> groupByKey(JavaRDD<JSONObject> results) {
		JavaPairRDD<String, Iterable<JSONObject>> pairRdd = results.mapToPair(f -> {
			Tuple2<String,JSONObject> tuple2 = null;
			JSONObject value = f.getJSONObject("columns");
			String statusId = value.getString("STATUS_ID");
			String statusValue = value.getString("STATUS_VALUE");
			String updateDate = value.getString("UPDATE_DATE");
			String updateTime = value.getString("UPDATE_TIME");
			String cityCode = value.getString("CITY_CODE");
			String deviceId = value.getString("DEVICE_ID");
			String beatDate = value.getString("BEAT_DATE");
			if(value.containsKey("BEAT_DATE")){
				try {
					Date date = DateUtils.parseDateTime(beatDate);
					long timestamp = date.getTime();
					String key = cityCode + "_" + deviceId + "_" + Constants.FAILURE_TYPE_OFFLINE;
					JSONObject json = new JSONObject();
					json.put("city_code", cityCode);
					json.put("key", key);
					json.put("device_id", deviceId);
					json.put("timestamp", timestamp);
					json.put("status_value", 0);
					value.put("timestamp", timestamp);
					json.put("value", value);
					tuple2 = new Tuple2<String,JSONObject>(key, json);
				} catch (Exception e) {
					tuple2 = new Tuple2<String,JSONObject>(null,null);
				}
			} else {
				String datestr = updateDate + updateTime;
				Date date;
				try {
					date = DateUtils.parseDate(datestr, "yyyyMMddHHmmss");
					long timestamp = date.getTime();
					String key = cityCode + "_" + deviceId + "_" + Constants.FAILURE_TYPE_FAILURE;
					//如果是故障状态，则在key中加入状态ID
					if(statusId != null && !"".equals(statusId)){
						key += "_" + statusId;
					}
					JSONObject json = new JSONObject();
					json.put("city_code", cityCode);
					json.put("key", key);
					json.put("device_id", deviceId);
					json.put("timestamp", timestamp);
					json.put("status_value", statusValue);
					value.put("timestamp", timestamp);
					json.put("value", value);
					tuple2 = new Tuple2<String,JSONObject>(key, json);
				} catch (Exception e) {
					tuple2 = new Tuple2<String,JSONObject>(null,null);
				}
			}
			return tuple2;
		}).groupByKey();
		return pairRdd;
	}

	/**
	 * 处理并写入数据
	 * @param pairRdd
	 */
	private void process(JavaPairRDD<String, Iterable<JSONObject>> pairRdd) {
		pairRdd.foreachPartition(f -> {
			FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
			FailureDetailsService failureDetailsService = (FailureDetailsService)ContextAccessor.getBean("failureDetailsService");
			RedissonClient redisson = RedisUtils.getRedissonClient();
			while (f.hasNext()) {
				Tuple2<String,Iterable<JSONObject>> tuple = f.next();
				String key = tuple._1;
				if(key == null || "".equals(key)){
					continue;
				}
				Iterable<JSONObject> it = tuple._2;
				List<JSONObject> list = new ArrayList<JSONObject>();
				for(JSONObject json : it){
					list.add(json);
				}
				String keys [] = key.split("_");
				String failureType = keys[2];
				if(failureType.equals(Constants.FAILURE_TYPE_FAILURE)){
					//批量实时处理中，最新的一条记录才有效。
					sortList(list);
					computingFailure(redisson, failureInfoService, failureDetailsService, list);
				} else if(failureType.equals(Constants.FAILURE_TYPE_OFFLINE)) {
					sortList(list);
					computingHeartBeat(redisson,failureInfoService, failureDetailsService, list);
				}
			}
			
		});
	}

	private static void sortList(List<JSONObject> list) {
		Collections.sort(list, (o1, o2) -> {
			String city1 = o1.getString("city_code");
			String city2 = o2.getString("city_code");
			if(city1.compareTo(city2) > 0){
				return 1;
			} else if(city1.compareTo(city2) < 0){
				return -1;
			} else {
				String deviceId1 = o1.getString("device_id");
				String deviceId2 = o2.getString("device_id");
				if(deviceId1.compareTo(deviceId2) > 0){
					return 1;
				} else if(deviceId1.compareTo(deviceId2) == 0){
					JSONObject j1 = (JSONObject)o1;
					JSONObject j2 = (JSONObject)o2;
					long timestamp1 = j1.getLong("timestamp");
					long timestamp2 = j2.getLong("timestamp");
					if(timestamp1 > timestamp2){
						return 1;
					} else if(timestamp1 < timestamp2){
						return -1;
					} else {
						return 0;
					}
				} else {
					return -1;
				}
			}
		});
	}
	
	/**
	 * 推送故障数据
	 */
	public void publishData() {
		FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
		RedissonClient redisson = RedisUtils.getRedissonClient();
		publishDevices(redisson, failureInfoService);
		publishStationTypes(redisson, failureInfoService);
		
	}
	
	/**
	 * 处理并写入故障信息
	 * @param failureInfoService
	 * @param failureDetailsService
	 * @param list
	 */
	public static void computingFailure(RedissonClient redisson, FailureInfoService failureInfoService, FailureDetailsService failureDetailsService, List<JSONObject> list) {
		List<Map<String,FailureInfo>> infoList = new ArrayList<Map<String,FailureInfo>>();
		List<Map<String,FailureDetails>> detailList = new ArrayList<Map<String,FailureDetails>>();
		for(JSONObject json : list){
			JSONObject value = json.getJSONObject("value");
			String deviceId = value.getString("DEVICE_ID");
			String cityCode = value.getString("CITY_CODE");
			Device deviceInfo = DeviceCache.getInstance().get(cityCode, deviceId);
			//设备类型设置
			String deviceType = null;
			Constants.DeviceType type = null;
			if(deviceInfo != null){
				deviceType = deviceInfo.getDeviceType();
				if(Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI.getCode().equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}else if(Constants.DeviceType.DEVICE_TYPE_ZHAJI.getCode().equals(deviceType)){
					//闸机
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
			} else {
				deviceType = deviceId.substring(4,6);
				if("02".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}
				if("04".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
			}
			// String lastTimestamp = value.getString("T_LAST_TIMESTAMP");
			//故障状态处理
			if(value.containsKey("STATUS_ID")){
				String lineId = value.getString("LINE_ID");
				String stationId = value.getString("STATION_ID");
				String statusId = value.getString("STATUS_ID");
				String statusValue = value.getString("STATUS_VALUE");
				String updateDate = value.getString("UPDATE_DATE");
				String updateTime = value.getString("UPDATE_TIME");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				Date failureTime = null;
				try {
					failureTime = sdf.parse(updateDate + updateTime);
				} catch (ParseException e) {
					logger.error("故障信息错误：【更新日期 + 更新时间】格式错误！");
				}
				//如果状态是故障
				if(isFailure(statusId, statusValue)){
					//如果是主模块故障，写入故障表
					if("0000".equals(statusId)){
						//插入数据。
						FailureInfo fi = new FailureInfo();
						fi.setCity_code(cityCode);
						fi.setLine_code(lineId);
						fi.setStation_code(stationId);
						fi.setDevice_id(deviceId);
						fi.setCreate_time(new Date());
						fi.setStatus_value(statusValue);
						fi.setFailure_time(failureTime);
						fi.setDevice_type(type==null || type.getCode()==null?"":type.getCode());
						//故障类型与设备模式的状态值对应
						fi.setFailure_type(Constants.FAILURE_TYPE_FAILURE);
						fi = fillCacheField(cityCode, deviceId, fi);
						String key = "info-insert_" + cityCode + "_" + deviceId;
						//TODO 写入mysql和HBase记录
						Map<String,FailureInfo> infoMap = new HashMap<String, FailureInfo>();
						infoMap.put(key, fi);
						infoList.add(infoMap);
						//保存故障记录
						if(debug) logger.warn("记录故障：" + cityCode + "_" + deviceId);
						
					} else {
						DeviceStatusDic statusCache = DeviceStatusDicCache.getInstance().get(statusId, statusValue);
						if(statusCache == null){
							continue;
						} 
						FailureDetails fd = new FailureDetails();
						DeviceStatusDic statusMap = DeviceStatusDicCache.getInstance().get(statusId, statusValue);
						fd.setReason(statusMap != null && !"".equals(statusMap.getStatusValueName()) ? 
								statusMap.getStatusValueName().toString() : null);
						fd.setStatus_id(statusId);
						fd.setCity_code(cityCode);
						fd.setLine_code(lineId);
						fd.setStation_code(stationId);
						fd.setDevice_id(deviceId);
						fd.setCreate_time(new Date());
						fd.setStatus_value(statusValue);
						fd.setFailure_time(failureTime);
						fd = fillDetailsCacheField(cityCode, deviceId, fd);
						//保存故障明细
						String key = "detail-insert_" + cityCode + "_" + deviceId + "_" + statusId;
						//TODO 写入mysql和HBase记录
						Map<String,FailureDetails> detailMap = new LinkedHashMap<String, FailureDetails>();
						detailMap.put(key, fd);
						detailList.add(detailMap);
						if(debug) logger.warn("记录故障明细：" + cityCode + "_" + deviceId + "_" + statusId);
					}
				} else {//非故障处理
					//如果主状态非故障，删除故障记录和明细
					if("0000".equals(statusId)){
						FailureInfo fi = new FailureInfo();
						fi.setCity_code(cityCode);
						fi.setDevice_id(deviceId);
						fi.setFailure_type(Constants.FAILURE_TYPE_FAILURE);
						fi.setCreate_time(new Date());
						fi.setFailure_time(failureTime);
						fi.setStatus_value(statusValue); 
						fi = fillCacheField(cityCode, deviceId, fi);
						String key = "info-delete_" +  cityCode + "_" + deviceId;
						//TODO 写入mysql和HBase记录
						Map<String,FailureInfo> infoMap = new HashMap<String, FailureInfo>();
						infoMap.put(key, fi);
						infoList.add(infoMap);
					} else {	
						FailureDetails detail = new FailureDetails();
						detail.setCity_code(cityCode);
						detail.setDevice_id(deviceId);
						detail.setStatus_id(statusId);
						detail.setFailure_time(failureTime);
						detail.setStatus_value(statusValue);
						detail.setCreate_time(new Date());
						String key = "detail-delete_" + cityCode + "_" + deviceId;
						detail = fillDetailsCacheField(cityCode, deviceId, detail);
						//TODO 写入mysql和HBase记录
						Map<String,FailureDetails> detailMap = new LinkedHashMap<String, FailureDetails>();
						detailMap.put(key, detail);
						detailList.add(detailMap);
						if(debug) logger.warn("恢复故障记录：" + cityCode + "_" + deviceId);
					} 
				}
			}
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for(Map<String,FailureInfo> infoMap : infoList){
			for(String key: infoMap.keySet()){
				FailureInfo fi = infoMap.get(key);
				// 缓存时间
				try {
					publishLastTime(redisson, fi.getCity_code(), sdf, sdf.format(fi.getFailure_time()));
				} catch (Exception e) {
					logger.error(fi.getCity_code() + ":" + fi.getFailure_time());
				}
				if(key.startsWith("info-insert")){
					writeFailure(fi, null);
				}
				if(key.startsWith("info-delete")){
					writeFailure(fi, fi.getFailure_time());
				}
			}
		}
		for(Map<String,FailureDetails> detailMap : detailList){
			for(String key: detailMap.keySet()){
				FailureDetails fd = detailMap.get(key);
				//failureDetailsService.deleteByDeviceAndStatus(fd.getCity_code(), fd.getDevice_id(), fd.getStatus_id());
				if(key.startsWith("detail-insert")){
					writeFailureDetail(fd,null);
					if(debug) logger.warn("detail-insert..." + fd.getCity_code() + "_" + fd.getDevice_id() );
				}
				if(key.startsWith("detail-delete")){
					writeFailureDetail(fd, fd.getFailure_time());
					if(debug) logger.warn("detail-delete..." + fd.getCity_code() + "_" + fd.getDevice_id() );
				}
			}
		}
		
	}

	private static void writeFailureDetail(FailureDetails fd,
			Date recoverTime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		FailureDetailsHistory his = new FailureDetailsHistory();
		BeanUtils.copyProperties(fd, his);
		his.setRecover_time(recoverTime);
		saveOrUpdateFailureDetails(fd, his, sdf, recoverTime);
	}

	private static void saveOrUpdateFailureDetails(FailureDetails fd,
			FailureDetailsHistory fdp, SimpleDateFormat sdf, Date recoverTime) {
		FailureDetailsService failureDetailsService = (FailureDetailsService)ContextAccessor.getBean("failureDetailsService");
		FailureDetailsHistoryService failureDetailsHistoryService = (FailureDetailsHistoryService)ContextAccessor.getBean("failureDetailsHistoryService");
		Map<String,Object> pkMap = failureDetailsService.selectPK(fdp.getCity_code(), fdp.getDevice_id(), fdp.getStatus_id());
		if(recoverTime != null && !"".equals(recoverTime)){
			//恢复故障
			fdp.setRecover_time(recoverTime);
			if(pkMap != null && pkMap.size() > 0){
				Date date = (Date)pkMap.get("failure_time");
				//如果恢复时间小于故障时间，则删除故障记录（错误的故障记录）
				if(recoverTime.getTime() <= date.getTime()){
					failureDetailsHistoryService.delete(fdp);
					failureDetailsService.delete(fd);
				} else {
					//恢复故障
					failureDetailsHistoryService.recoverFailure(fdp);
					failureDetailsService.delete(fdp);
				}
			} else {
				//如果没有要恢复的实时故障明细，但历史表中存在，则恢复历史表中的故障明细
				List<Map<String,Object>> detailList = failureDetailsHistoryService.selectPK(fd.getCity_code(), fd.getDevice_id(), fd.getStatus_id());
				if(detailList != null &&  detailList.size() > 0){
					failureDetailsHistoryService.recoverFailure(fdp); 
				}
			}
		} else {
			if(pkMap == null){
				//写入故障
				fdp.setRecover_time(null); 
				failureDetailsHistoryService.insert(fdp);
				failureDetailsService.insert(fd);
			} else {
				//如果有故障上报时，如果查到大于当前上报时间的故障，则删除查出的故障，然后插入当前上报的故障
				Date failureTime = (Date)pkMap.get("failure_time");
				Date beatTime = fd.getFailure_time();
				if(beatTime.getTime() <= failureTime.getTime()){
					//数据时间小于故障中发生的时间，
					failureDetailsHistoryService.delete(fdp);
					failureDetailsHistoryService.insert(fdp);
					failureDetailsService.delete(fd);
					failureDetailsService.insert(fd);
				} else {
					//状态转变
					String statusValue = (String)pkMap.get("status_value");
					if(!fd.getStatus_value().equals(statusValue)){
						//更新到新的状态
						fdp.setStatus_value(fd.getStatus_value());
						failureDetailsHistoryService.updateStatus(fdp);
					}
				}
				
			}
		}
	}

	/**
	 * 写入历史数据
	 * @param fi
	 * @param isRecover
	 */
	public static void writeFailure(FailureInfo fi, Date recoverTime) {
		FailureInfoHistory failInfo = new FailureInfoHistory();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		BeanUtils.copyProperties(fi, failInfo);
		failInfo.setRecover_time(recoverTime);
		saveFailureInfo(fi, failInfo, sdf, recoverTime);
	}

	private static void saveFailureInfo(FailureInfo fi,
			FailureInfoHistory failInfo, SimpleDateFormat sdf, Date recoverTime) {
		FailureInfoHistoryService failureInfoHistoryService = (FailureInfoHistoryService)ContextAccessor.getBean("failureInfoHistoryService");
		FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
		Map<String,Object> pkMap = failureInfoService.selectPK(fi.getCity_code(), fi.getDevice_id(), fi.getFailure_type());
		//恢复故障
		if(recoverTime != null && !"".equals(recoverTime)){
			failInfo.setRecover_time(recoverTime);
			if(pkMap != null && pkMap.size() > 0){
				Date date = (Date)pkMap.get("failure_time");
				//如果恢复时间小于故障时间，则删除故障记录（错误的故障记录）
				if(recoverTime.getTime() <= date.getTime()){
					failureInfoHistoryService.delete(failInfo);
					failureInfoService.delete(fi);
				} else {
					//恢复故障
					failureInfoHistoryService.recoverFailure(failInfo);
					failureInfoService.delete(fi);
				}
			} else {
				//如果实时故障中不存在，但历史表中存在故障，则恢复历史表中的故障
				pkMap = failureInfoHistoryService.selectPK(fi.getCity_code(), fi.getDevice_id(), fi.getFailure_type());
				if(pkMap != null &&  pkMap.size() > 0){
					failureInfoHistoryService.recoverFailure(failInfo); 
				}
			}
		} else {//插入故障
			//如果未查到记录，则新插入故障信息
			if(pkMap == null){
				String statusValue = fi.getStatus_value();
				if(!Constants.FAILURE_STATUS_NORMAL.equals(statusValue)){
					//写入故障
					failInfo.setFailure_time(fi.getFailure_time());
					failInfo.setRecover_time(null);
					failureInfoHistoryService.insert(failInfo);
					failureInfoService.insert(fi);
				}
			} else {
				//如果查到PK存在两种情况：1.可能是错误数据,服务停止造成的错误数据。2.可能是故障状态的切换，
				//第一种情况的处理,删除错误数据，并写入新故障数据
				Date failureTime = (Date)pkMap.get("failure_time");
				Date beatTime = fi.getFailure_time();
				if(beatTime.getTime() <= failureTime.getTime()){
					//数据时间小于故障中发生的时间，
					failureInfoHistoryService.delete(failInfo);
					failureInfoHistoryService.insert(failInfo);
					failureInfoService.delete(fi);
					failureInfoService.insert(fi);
				} else {
					//第二种情况，更新故障状态
					String statusValue = (String)pkMap.get("status_value");
					if(!fi.getStatus_value().equals(statusValue)){
						//更新到新的状态
						failInfo.setStatus_value(fi.getStatus_value());
						failureInfoHistoryService.updateStatus(failInfo);
						failureInfoService.insert(fi);
					}
				}
			}
		} 
		
	}
	
	/**
	 * 心跳数据处理
	 * @param redisson
	 * @param failureInfoService
	 * @param failureDetailsService
	 * @param list
	 */
	public static void computingHeartBeat(RedissonClient redisson,
			FailureInfoService failureInfoService,
			FailureDetailsService failureDetailsService, List<JSONObject> list) {
		List<Map<String,FailureInfo>> infoList = new ArrayList<Map<String,FailureInfo>>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for(JSONObject json : list){
			JSONObject value = json.getJSONObject("value");
			String deviceId = value.getString("DEVICE_ID");
			String cityCode = value.getString("CITY_CODE");
			String beatDate = value.getString("BEAT_DATE");
			int count = 0; 
			//取出redis中key的次数
			RBucket<JSONObject> bucket = getRedisBeatBucket(redisson,cityCode,deviceId);
			if(!bucket.isExists() || bucket == null){
				//如果没有该设备的心跳key,则记录当前心跳时间，并记数为1。
				JSONObject j = new JSONObject();
				j.put("timestamp", beatDate);
				count = 1;
				j.put("count", count); 
				bucket.set(j);
				//推送时间
				publishLastTime(redisson, cityCode, sdf, beatDate);
			} else {//如果redisKey中存在该设备，则判断恢复的次数
				JSONObject j = bucket.get();
				String keytimestr = j.getString("timestamp");
				Date beattime = null;
				Date keytime = null;
				try {
					beattime = sdf.parse(beatDate);
					keytime = sdf.parse(keytimestr);
				} catch (ParseException e) {
					logger.error("错误数据：心跳时间值错误(" + 
							cityCode + "---" + deviceId + "---" + beatDate + ")!");
				}
				//判断心跳与缓存的时间间隔，如果大于过期时间，则写入历史
				if(beattime.getTime() - keytime.getTime() >= Constants.HEART_EXPIRE_MINUTES * 60 * 1000){
					FailureInfo fi = new FailureInfo();
					fi.setCity_code(cityCode);
					fi.setDevice_id(deviceId);
					fi.setFailure_type(Constants.FAILURE_TYPE_OFFLINE);
					fi.setStatus_value(Constants.FAILURE_STATUS_OFFLINE);
					fi.setFailure_time(beattime); 
					fi.setCreate_time(new Date()); 
					fi = fillCacheField(cityCode, deviceId, fi);
					j.put("timestamp", beatDate);
					j.put("count", 1);
					String key = "info-insert_" + cityCode + "_" + deviceId;
					//TODO 写入mysql和HBase记录
					Map<String,FailureInfo> infoMap = new HashMap<String, FailureInfo>();
					infoMap.put(key, fi);
					infoList.add(infoMap);
				} else {
					count = j.getIntValue("count");
					count ++;
					//如果计数的值等于3，则删除实时故障记录
					if(count == Constants.FAILOVER_HEART_BEAT_TIMES){
						FailureInfo fi = new FailureInfo();
						String key = "info-delete_" + cityCode + "_" + deviceId;
						fi.setCity_code(cityCode);
						fi.setDevice_id(deviceId);
						fi.setFailure_type(Constants.FAILURE_TYPE_OFFLINE);
						fi.setFailure_time(beattime);
						fi.setStatus_value(Constants.FAILURE_STATUS_OFFLINE);
						fi.setCreate_time(new Date());
						//设置缓存字段
						fi = fillCacheField(cityCode, deviceId, fi);
						//TODO 写入mysql和HBase记录
						Map<String,FailureInfo> infoMap = new HashMap<String, FailureInfo>();
						infoMap.put(key, fi);
						infoList.add(infoMap);
						j.put("count", Constants.FAILOVER_HEART_BEAT_TIMES);
						j.put("timestamp", beatDate);
						if(debug) logger.warn("清除离线记录：" + cityCode + "_" + deviceId);
					} else if(count < Constants.FAILOVER_HEART_BEAT_TIMES){
						//如果小于3，更新时间和计数
						j.put("timestamp", beatDate);
						j.put("count", count);
					} else {
						//如果大于3，只更新时间，不改变计数
						j.put("timestamp", beatDate);
						j.put("count", Constants.FAILOVER_HEART_BEAT_TIMES);
					}
				}
				bucket.set(j);
			}
			//更新过期时间
			bucket.expire(Constants.HEART_EXPIRE_MINUTES, TimeUnit.MINUTES);
			// 在redis中记录当前数据的心跳时间，并推送该时间到前端
			publishLastTime(redisson, cityCode, sdf, beatDate);
		}
		
		for(Map<String,FailureInfo> infoMap: infoList){
			for(String key: infoMap.keySet()){
				FailureInfo fi = infoMap.get(key);
				if(key.startsWith("info-delete")){
					writeFailure(fi, fi.getFailure_time());
					if(debug) logger.warn("info-delete..." + fi.getCity_code() + "_" + fi.getDevice_id() );
				}
				if(key.startsWith("info-insert")){
					writeFailure(fi, null);
					if(debug) logger.warn("info-insert..." + fi.getCity_code() + "_" + fi.getDevice_id() );
				}
			}
		}	
	}

	/**
	 * 发布站点故障信息
	 * @param failureInfoService
	 */
	public static void publishStationTypes(RedissonClient redisson, FailureInfoService failureInfoService) {
		
		//站点设备汇总
		List<Map<String,Object>> deviceCountMaps = failureInfoService.queryDeviceTypeCount();
		//故障分类汇总
		List<Map<String,Object>> failureTypeCountMap = failureInfoService.queryFailureDeviceTypeCount();
		//站点故障汇总
		List<Map<String,Object>> stationDeviceCountMap = failureInfoService.queryStationDeviceTypeCount();
		//设备离线跟其他故障重复数据汇总
		Map<String,Object> mapFalg=failureInfoService.queryOfflineAndOtherRepeatCount();
		
		//设备汇总
		Map<String, JSONObject> deviceCountTotalMap = new HashMap<String, JSONObject>();
		for(Map<String,Object> m: deviceCountMaps){
			String city_code = (String)m.get("city_code");
			BigDecimal station_num = (BigDecimal)m.get("station_num");
			BigDecimal device_num = (BigDecimal)m.get("device_num");
			JSONObject json = new JSONObject();
			json.put("failure_station_num", station_num);
			json.put("failure_device_num", device_num);
			deviceCountTotalMap.put(city_code, json);
		}
	
		//各站点汇总 Map<city_code, Map<station_code, JSONObject>>
		Map<String, Map<String,JSONObject>> stationMap = new HashMap<String, Map<String,JSONObject>>();
		for(Map<String,Object> m : stationDeviceCountMap){
			String city_code = (String)m.get("city_code");
			String station_code = (String)m.get("station_code");
			String status_value = (String)m.get("status_value");
			String station_name = (String)m.get("station_name");
			BigDecimal ticket_num = (BigDecimal)m.get("ticket_num");
			BigDecimal gate_num = (BigDecimal)m.get("gate_num");
			
			JSONObject numj = new JSONObject();
			numj.put("ticket_num", ticket_num.longValue());
			numj.put("gate_num", gate_num.longValue());
			numj.put("total_num", ticket_num.longValue() + gate_num.longValue());
			
			Map<String,JSONObject> station = stationMap.get(city_code);
			if(station == null){
				station = new HashMap<String,JSONObject>();
			}
			JSONObject j = (JSONObject)station.get(station_name);
			if(j == null){
				j = new JSONObject();
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
			j.put("station_id", station_code);
			j.put("station_name", station_name);
			station.put(station_name, j);
			stationMap.put(city_code, station);
		}
		
		//各类型汇总
		Map<String,JSONObject> typeMaps = new HashMap<String,JSONObject>();
		for(Map<String, Object> typeMap : failureTypeCountMap){
			String city_code = (String)typeMap.get("city_code");
			String status_value = (String)typeMap.get("status_value");
			BigDecimal ticket_num = (BigDecimal)typeMap.get("ticket_num");
			BigDecimal gate_num = (BigDecimal)typeMap.get("gate_num");
			JSONObject numj = new JSONObject();
			numj.put("ticket_num", ticket_num.longValue());
			numj.put("gate_num", gate_num.longValue());
			numj.put("total_num", ticket_num.longValue() + gate_num.longValue());
			
			JSONObject j = typeMaps.get(city_code);
			if(j == null){
				j = new JSONObject();
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
				numj.put("GPJ_stopRepeat", mapFalg==null || mapFalg.get("stop_gpj")==null?0:mapFalg.get("stop_gpj"));
				numj.put("GPJ_repairRepeat", mapFalg==null || mapFalg.get("repair_gpj")==null?0:mapFalg.get("repair_gpj"));
				numj.put("GPJ_failureRepeat", mapFalg==null || mapFalg.get("failure_gpj")==null?0:mapFalg.get("failure_gpj"));
				numj.put("GPJ_interruptRepeat", mapFalg==null || mapFalg.get("interrupt_gpj")==null?0:mapFalg.get("interrupt_gpj"));
				numj.put("ZJ_stopRepeat", mapFalg==null || mapFalg.get("stop_zj")==null?0:mapFalg.get("stop_zj"));
				numj.put("ZJ_repairRepeat", mapFalg==null || mapFalg.get("repair_zj")==null?0:mapFalg.get("repair_zj"));
				numj.put("ZJ_failureRepeat", mapFalg==null || mapFalg.get("failure_zj")==null?0:mapFalg.get("failure_zj"));
				numj.put("ZJ_interruptRepeat", mapFalg==null || mapFalg.get("interrupt_zj")==null?0:mapFalg.get("interrupt_zj"));
				j.put("device_offline_num", numj);
			}
			typeMaps.put(city_code, j);
		}
		
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		for(String city_code : stationMap.keySet()){
			JSONObject map = typeMaps.get(city_code);
			JSONObject json = new JSONObject();
			if(map != null){
				for(String failureType : map.keySet()){
					json.put(failureType, map.get(failureType));
				}
			}
			JSONObject totalJson = deviceCountTotalMap.get(city_code);
			json.put("failure_station_num", totalJson.get("failure_station_num"));
			json.put("failure_device_num", totalJson.get("failure_device_num"));
			Map<String,JSONObject> station = (Map<String,JSONObject>) stationMap.get(city_code);
			JSONArray array = new JSONArray();
			for(String key : station.keySet()){
				JSONObject j = station.get(key);
				array.add(j);
			}
			json.put("station", array);
			redisson.getTopic("device_failure_station_" + city_code).publish(json.toJSONString());
			System.out.println("device_failure_station_" + city_code+"-------"+json.toJSONString());
		}	
	}
	
	/**
	 * 发布设备故障信息
	 * @param failureInfoService
	 */
	public static void publishDevices(RedissonClient redisson,FailureInfoService failureInfoService
			) {
		//查询所有故障，并推送故障信息
		Map<String, JSONArray> recordMap = new HashMap<String, JSONArray>();
		List<FailureInfo> failureList = failureInfoService.queryList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for(FailureInfo failure : failureList){
			String cityCode = failure.getCity_code();
			if(cityCode == null || "".equals(cityCode)){
				continue;
			}
			JSONArray array = recordMap.get(cityCode);
			if(array == null){
				array = new JSONArray();
			}
			JSONObject json = new JSONObject(); 
			json.put("station_id", failure.getStation_code());
			json.put("station_name", failure.getStation_name());
			json.put("device_id", failure.getDevice_id());
			json.put("failure_time", sdf.format(failure.getFailure_time()));
			json.put("failure_type", failure.getFailure_type());
			json.put("status_value", failure.getStatus_value());
			array.add(json);
			recordMap.put(cityCode, array);
		}
		for(String city_code : recordMap.keySet()){
			JSONArray array = recordMap.get(city_code);
			redisson.getTopic("device_failure_" + city_code).publish(array.toJSONString());
		}
	}
	
	public static void listenHeartBeat(){
		//注册过期事件
		RTopic<String> topic = RedisUtils.getRedissonClient().getTopic("__keyevent@0__:expired", StringCodec.INSTANCE);
		topic.addListener(new MessageListener<String>() {
			@Override
			public void onMessage(String channel, String msg) {
				String arr [] = msg.split(":");
				String cityCode = arr[1];
				String deviceId = arr[2];
				Date beatDate = new Date();
				FailureInfo fi = new FailureInfo();
				fi.setDevice_id(deviceId);
				fi.setCity_code(cityCode);
				fi.setStatus_value(Constants.FAILURE_STATUS_OFFLINE);
				fi.setFailure_type(Constants.FAILURE_TYPE_OFFLINE);
				fi = fillCacheField(cityCode, deviceId, fi);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String times = sdf.format(beatDate);
				try {
					Date datetime = sdf.parse(times);
					fi.setFailure_time(datetime); 
					fi.setCreate_time(datetime);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//记录故障
				writeFailure(fi, null);
				if(debug) logger.warn("记录离线故障：" + cityCode + "_" + deviceId);
			}
		});
	}
	
	private static FailureInfo fillCacheField(String cityCode, String deviceId,
			FailureInfo fi) {
		Device deviceInfo = DeviceCache.getInstance().get(cityCode, deviceId);
		//设备类型设置
		String deviceType = null;
		Constants.DeviceType type = null;
		fi.setCity_code(cityCode);
		fi.setDevice_id(deviceId);
		City city = CityCache.getInstance().get(cityCode);
		if(deviceInfo != null){
			deviceType = deviceInfo.getDeviceType();
			String areaCode = deviceInfo.getAreaCode();
			fi.setArea_code(areaCode);
			if(deviceType==null || "".equals(deviceType)){
				deviceType = deviceId.substring(4,6);
				if("02".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}
				if("04".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
				deviceInfo.setDeviceType(type.getCode());
			}else{
				if(Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI.getCode().equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}else if(Constants.DeviceType.DEVICE_TYPE_ZHAJI.getCode().equals(deviceType)){
					//闸机
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
			}
			String deviceTypeName=null;
			if(type!=null){
				deviceTypeName=type.getName()==null?null:type.getName();
			}
			//取自缓存的字段
			fi.setCity_name(city == null ? null : city.getName());
			fi.setLine_code(deviceInfo.getLineCode());
			fi.setLine_name(deviceInfo.getLineNameZh());
			fi.setStation_code(deviceInfo.getStationCode());
			fi.setStation_name(deviceInfo.getStationNameZh());
			fi.setDevice_name(deviceInfo.getDeviceName());
			fi.setDevice_type_name(deviceTypeName);
			fi.setDevice_type(deviceInfo.getDeviceType());
			fi.setArea_code(deviceInfo.getAreaCode());
		} else {
			deviceType = deviceId.substring(4,6);
			if("02".equals(deviceType)){
				type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
			} else if("04".equals(deviceType)){
				type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
			} 
			String stationCode = deviceId.substring(0,4);
			Station station = StationCache.getInstance().get(cityCode, stationCode);
			fi.setCity_name(city == null ? null : city.getName());
			fi.setLine_name(station.getLineNameZh());
			fi.setStation_code(stationCode); 
			fi.setStation_name(station.getStationNameZh());
			fi.setDevice_type(type==null || type.getCode()==null?null:type.getCode());
			fi.setDevice_type_name(type==null || type.getName()==null?null:type.getName());
		}
		return fi;
	}
	
	private static FailureDetails fillDetailsCacheField(String cityCode, String deviceId,
			FailureDetails fd) {
		fd.setCity_code(cityCode);
		fd.setDevice_id(deviceId); 
		Device deviceInfo = DeviceCache.getInstance().get(cityCode, deviceId);
		//设备类型设置
		String deviceType = null;
		Constants.DeviceType type = null;
		City city = CityCache.getInstance().get(cityCode);
		if(deviceInfo != null){
			deviceType = deviceInfo.getDeviceType();
			if(deviceType==null || "".equals(deviceType)){
				deviceType = deviceId.substring(4,6);
				if("02".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}
				if("04".equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
				deviceInfo.setDeviceType(type.getCode());
			}else{
				if(Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI.getCode().equals(deviceType)){
					type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
				}else if(Constants.DeviceType.DEVICE_TYPE_ZHAJI.getCode().equals(deviceType)){
					//闸机
					type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
				}
			}
			String deviceTypeName=null;
			if(type!=null){
				deviceTypeName=type.getName()==null?null:type.getName();
			}
			//取自缓存的字段
			fd.setCity_name(city == null ? null : city.getName());
			fd.setLine_code(deviceInfo.getLineCode());
			fd.setLine_name(deviceInfo.getLineNameZh());
			fd.setStation_code(deviceInfo.getStationCode());
			fd.setStation_name(deviceInfo.getStationNameZh());
			fd.setDevice_name(deviceInfo.getDeviceName());
			fd.setDevice_type_name(deviceTypeName);
			fd.setDevice_type(deviceInfo.getDeviceType());
		} else {
			deviceType = deviceId.substring(4,6);
			if("02".equals(deviceType)){
				type = Constants.DeviceType.DEVICE_TYPE_GOUPIAOJI;
			}
			if("04".equals(deviceType)){
				type = Constants.DeviceType.DEVICE_TYPE_ZHAJI;
			}
			String stationCode = deviceId.substring(0,4);
			Station station = StationCache.getInstance().get(cityCode, stationCode);
			fd.setCity_name(city == null ? null : city.getName());
			fd.setLine_name(station.getLineNameZh());
			fd.setStation_code(stationCode); 
			fd.setStation_name(station.getStationNameZh());
			fd.setDevice_type(type==null || type.getCode()==null?null:type.getCode());
			fd.setDevice_type_name(type==null || type.getName()==null?null:type.getName());
		}
		return fd;
	}

	
	
	/**
	 * 是否是故障
	 * @param statusValue
	 * @return
	 */
	public static boolean isFailure(String statusId, String statusValue) {
		if("0000".equals(statusId)){
			if("0".equals(statusValue)){
				return false;
			} else {
				return true;
			}
		} else {
			if("0".equals(statusValue)){
				return false;
			}
			//运营状态
			if("101".equals(statusId)){
				if("2".equals(statusValue) || "5".equals(statusValue)){
					return true;
				} else {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * 读取心跳恢复次数
	 * @param redisson
	 * @param cityCode
	 * @param deviceId
	 * @return
	 */
	private static RBucket<JSONObject> getRedisBeatBucket(RedissonClient redisson, String cityCode, String deviceId) {
		String prefix = "heartbeat:" + cityCode;
		RBucket<JSONObject> bucket = redisson.getBucket(prefix + ":" + deviceId);
		return bucket;
	}
	
	private static void publishLastTime(RedissonClient redisson, String cityCode,
			SimpleDateFormat sdf, String date) {
		if(date != null){
			RBucket<String> bucket = redisson.getBucket("lastTime");
			String time = bucket.get();
			if(time != null && !"".equals(time)){
				if(date.compareTo(time) > 0){
					bucket.set(date);
					JSONObject json = new JSONObject();
					json.put("lastTime", date);
					redisson.getTopic("monitoring-init-time").publish(json.toJSONString());
				}
			} else {
				bucket.set(date);
			}
		}
	}

	/**
	 * 过滤数据
	 * @param col
	 * @return
	 */
	public static boolean filterData(JSONObject col, String timestamp){
		String tableName = col.getString("tableName");
		JSONObject columns = col.getJSONObject("columns");
		boolean hasLastTimestamp = columns.containsKey("T_LAST_TIMESTAMP");
		String lasttime = columns.getString("T_LAST_TIMESTAMP");
		long lasttimestamp = getLongTimestamp(lasttime);
		//数据过滤: 有最新更新时间和设备ID
		if(hasLastTimestamp){
			if(timestamp != null && !"".equals(timestamp)){
				if(lasttimestamp > getLongTimestamp(timestamp)){
					return true;
				} else {
					return false;
				}
			}
			if("SKP:DEVICE_HEART_BEAT".equals(tableName) && columns.containsKey("BEAT_DATE")){
				String beatDate = columns.getString("BEAT_DATE");
				if(beatDate != null && !"".equals(beatDate)){
					return true;
				}
			}
			if("SKP:DEVICE_STATUS".equals(tableName) && columns.containsKey("STATUS_ID") ){
				String updateDate = columns.getString("UPDATE_DATE");
				String updateTime = columns.getString("UPDATE_TIME");
				if(updateDate == null || "".equals(updateDate)){
					return false;
				}
				if(updateTime == null || "".equals(updateTime)){
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	/**
	 * 传入时间戳的处理
	 * @param timestamp
	 * @return
	 */
	public static long getLongTimestamp(String timestamp){
		if(timestamp == null || "".equals(timestamp)){
			return 0;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse(timestamp);
			return date.getTime();
		} catch (ParseException e) {
			sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				date = sdf.parse(timestamp);
				return date.getTime();
			} catch (ParseException e1) {
				return Long.parseLong(timestamp);
			}
		}
	}
	/**
	 * 验证离线设备数据
	 * 对比redis中缓存设备信息和心跳key数据，筛选实时数据中设备没有缓存到redis中，但上传心跳的数据
	* @Title: compareDeviceAndHeart 
	* @author yaoshijie  
	* @Description: TODO
	* @param     参数  
	* @return void    返回类型  
	* @throws
	 */
	public static void compareDeviceAndHeart(){
		FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
		Map<String,Device> deviceCacheMap=DeviceCache.getInstance().getCacheMap();
		String device_ids="";
		for (String key : deviceCacheMap.keySet()){
			Device bean = deviceCacheMap.get(key);
			String device_id = bean.getDeviceId();
			if("".equals(device_ids)){
				device_ids=device_id;
			}else{
				device_ids+=","+device_id;
			}
		}
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RKeys rkeys = redisson.getKeys();
		Iterable<String> its = rkeys.getKeysByPattern("heartbeat:*");
		Iterator<String> it = its.iterator();
		String result="";
		while(it.hasNext()){
			String keystr = it.next();
			String [] keys = keystr.split(":");
			String deviceId = keys[2];
			if(device_ids.indexOf(deviceId)<0){
				if("".equals(result)){
					result=deviceId;
				}else{
					result+=","+deviceId;
				}
			}
		}
		System.out.println("缓存设备和心跳设备差异---："+result);
		List<Map<String, Object>> list=failureInfoService.queryFailureData("4401", "9");
		String offlineFromMysql="";
		for(Map<String,Object> mapB:list){
			String d=(String) mapB.get("device_id");
			if(device_ids.indexOf(d)<0){
				if("".equals(offlineFromMysql)){
					offlineFromMysql=d;
				}else{
					offlineFromMysql+=","+d;
				}
			}
		}
		System.out.println("缓存是设备和数据库故障设备差异---："+offlineFromMysql);
	}

}
