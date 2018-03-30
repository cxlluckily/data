package com.shankephone.data.storage.convertor;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public interface Convertible {
	
	/**
	 * 按自定规则处理，支持多字段
	 * @param json
	 * @return
	 */
	public Map<String,String> getValue(String name, JSONObject json);

}
