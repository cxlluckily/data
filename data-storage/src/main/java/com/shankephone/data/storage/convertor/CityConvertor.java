package com.shankephone.data.storage.convertor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.storage.exception.SyncException;
import com.shankephone.data.storage.spark.SyncTaskExecutor;

public class CityConvertor implements Convertible{
	private static Logger logger = LoggerFactory.getLogger(CityConvertor.class);
	private static Map<String,String> cityMap = new HashMap<String,String>();
	private static Pattern cityCodePattern = Pattern.compile("\\d{4}");
	private static String citycodeDefault = "0000";
	
	static {
		cityMap.put("5100","4401");
		cityMap.put("STTRADE","4401");
		cityMap.put("STTRADE_MWS","4401");
		cityMap.put("CS_METRO","4100");
	}
	
	@Override
	public Map<String,String> getValue(String name, JSONObject json){
		if(json == null){
			throw new SyncException("自定义规则处理：[" + json + "]为空！");
		}
		String columnValue = json.getString(name);
		Matcher m = cityCodePattern.matcher(columnValue);
		String cityCode = null;
		if(m.find()){
			String cityCodeTemp = m.group(0);
			cityCode = cityMap.get(cityCodeTemp);
			if(cityCode == null){
				cityCode = cityCodeTemp;
			}
		} else {
			cityCode = cityMap.get(columnValue);
			if (cityCode == null) {
				logger.warn("未找到有效的城市代码，数据为：[" + name + "],[" + json + "]");
				cityCode = citycodeDefault;
			}
		}
		Map<String,String> map = new HashMap<String,String>();
		map.put(name, cityCode);
		return map;
	}
	
	public static void main(String[] args) {
		CityConvertor convertor = new CityConvertor();
		JSONObject json = new JSONObject();
		json.put("schemaName", "STTRADE");
		JSONObject ch = new JSONObject();
		ch.put("partner_id", "51000000001_0005");
		json.put("columns", ch);
		
		System.out.println(json.toJSONString());
		Map<String,String> m = convertor.getValue("schemaName", json);
		System.out.println(m);
		Map<String,String> m1 = convertor.getValue("partner_id", json.getJSONObject("columns"));
		System.out.println(m1);
	}

}
