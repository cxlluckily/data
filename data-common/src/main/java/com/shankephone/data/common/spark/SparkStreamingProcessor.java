package com.shankephone.data.common.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.alibaba.fastjson.JSONObject;

public interface SparkStreamingProcessor {
	
	public default void beforeProcess(JavaStreamingContext context, Map<String, Object> args){
		
	};
	
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd, Map<String, Object> args);
	
}
