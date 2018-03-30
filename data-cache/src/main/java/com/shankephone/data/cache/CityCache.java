package com.shankephone.data.cache;
/**
 * 城市代码缓存
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:27:45
 */
public class CityCache extends AbstractMapCache<City> {
	
	private static final CityCache CACHE = new CityCache(City.class, "SKP:CITY_CODE", "city_code", "code");
	
	public CityCache(Class<City> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		super(clazz, hbaseTable, redisKey, mapKeyPropertyName);
	}

	
	public static CityCache getInstance() {
		return CACHE;
	}
	
}
