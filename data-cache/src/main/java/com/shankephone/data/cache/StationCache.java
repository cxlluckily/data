package com.shankephone.data.cache;

import java.util.List;
/**
 * 站点缓存
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:28:38
 */
public class StationCache extends AbstractMapCache<Station> {
	
	private static final StationCache CACHE = new StationCache(Station.class, "SKP:STATION_CODE", "station_code", "cityCode", "stationCode");

	public StationCache(Class<Station> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		super(clazz, hbaseTable, redisKey, mapKeyPropertyName);
	}

	public static StationCache getInstance() {
		return CACHE;
	}

	@Override
	protected void fillObject(List<Station> objList) {
		for (Station station : objList) {
			City city = CityCache.getInstance().get(station.getCityCode());
			Line line = LineCache.getInstance().get(station.getCityCode(), station.getLineCode());
			if (city != null) {
				station.setCityName(city.getName());
			}
			if (line != null) {
				station.setLineNameZh(line.getLineNameZh());
			}
		}
	}
	
}
