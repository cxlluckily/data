package com.shankephone.data.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 设备信息缓存
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:27:57
 */
public class DeviceCache extends AbstractMapCache<Device> {
	
	private static final DeviceCache CACHE = new DeviceCache(Device.class, "SKP:DEVICE_INFO", "device_info", "cityCode", "deviceId");
	/**
	 * 各站点的所有设备
	 */
	private Map<String, List<Device>> stationMap = new HashMap<>();
	private Map<String, List<Device>> stationNameMap = new HashMap<>();
	
	public DeviceCache(Class<Device> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		super(clazz, hbaseTable, redisKey, mapKeyPropertyName);
	}

	public static DeviceCache getInstance() {
		return CACHE;
	}
	/**
	 * 按站点获取所有设备
	 * @author DuXiaohua
	 * @date 2017年11月9日 下午6:02:04
	 * @param cityCode
	 * @param stationCode
	 * @return
	 */
	public List<Device> getByStation(String cityCode, String stationCode) {
		//确保本地缓存被加载
		getCacheMap();
		return stationMap.get(cityCode + "_" + stationCode);
	}
	
	/**
	 * 按站点名称获取所有设备
	 * @author DuXiaohua
	 * @date 2017年11月9日 下午6:02:04
	 * @param cityCode
	 * @param stationCode
	 * @return
	 */
	public List<Device> getByStationName(String cityCode, String stationName) {
		//确保本地缓存被加载
		getCacheMap();
		return stationNameMap.get(cityCode + "_" + stationName);
	}
	
	@Override
	protected void afterRefreshLocal() {
		super.afterRefreshLocal();
		cacheMap.forEach((key, value) -> {
			String cityCode = value.getCityCode();
			String stationCode = value.getStationCode();
			String cityStation = cityCode + "_" + stationCode;
			List<Device> deviceList = stationMap.get(cityStation);
			if (deviceList == null) {
				deviceList = new ArrayList<>();
				stationMap.put(cityStation, deviceList);
			}
			deviceList.add(value);
			
			String stationName = value.getStationNameZh();
			String cityStationName = cityCode + "_" + stationName;
			List<Device> deviceListStationName = stationNameMap.get(cityStationName);
			if (deviceListStationName == null) {
				deviceListStationName = new ArrayList<>();
				stationNameMap.put(cityStationName, deviceListStationName);
			}
			deviceListStationName.add(value);
		});
	}

	@Override
	protected void fillObject(List<Device> objList) {
		for (Device device : objList) {
			City city = CityCache.getInstance().get(device.getCityCode());
			Line line = LineCache.getInstance().get(device.getCityCode(), device.getLineCode());
			Station station = StationCache.getInstance().get(device.getCityCode(), device.getStationCode());
			if (city != null) {
				device.setCityName(city.getName());
			}
			if (line != null) {
				device.setLineNameZh(line.getLineNameZh());
			}
			if (station != null) {
				device.setStationNameZh(station.getStationNameZh());
			}
		}
	}
	 
}
