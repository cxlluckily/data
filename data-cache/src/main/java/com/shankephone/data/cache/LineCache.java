package com.shankephone.data.cache;

import java.util.List;
/**
 * 线路缓存
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:28:29
 */
public class LineCache extends AbstractMapCache<Line> {
	
    private static final LineCache CACHE = new LineCache(Line.class, "SKP:LINE_CODE", "line_code", "cityCode", "lineCode");
	
	public LineCache(Class<Line> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		super(clazz, hbaseTable, redisKey, mapKeyPropertyName);
	}
	
	public static LineCache getInstance() {
		return CACHE;
	}
	
	@Override
	protected void fillObject(List<Line> objList) {
		for (Line line : objList) {
			City city = CityCache.getInstance().get(line.getCityCode());
			if (city != null) {
				line.setCityName(city.getName());
			}
		}
	}

}
