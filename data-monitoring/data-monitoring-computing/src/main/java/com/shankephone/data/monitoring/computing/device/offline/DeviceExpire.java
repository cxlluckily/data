package com.shankephone.data.monitoring.computing.device.offline;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.StringCodec;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.monitoring.computing.util.Constants;

public class DeviceExpire {
	
	/**
	 * 在redis中初始化设备TTL
	 * 若 select_city_code 为null 则初始化所有城市设备
	 * @author 森
	 * @date 2017年10月11日 上午11:12:03
	 * @param select_city_code
	 */
	public void initExpire(SparkSQLBuilder builder, String select_city_code){
//		SparkSQLBuilder builder = new SparkSQLBuilder("heartbeat_init");
		Map<String, Object> sqlParams = new HashMap<>();
		if (select_city_code!=null)
			sqlParams.put("select_city_code", "'"+select_city_code+"'");
		Dataset<Row> results = builder.executeSQL("heartbeat_init", sqlParams);
		
		List<Row> rows = results.collectAsList();
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RKeys keys = redisson.getKeys();
		String namespacePattern = "heartbeat:"+(select_city_code==null?"":select_city_code)+"*";
		Collection<String> deleteKeys = keys.findKeysByPattern(namespacePattern);
		for (String key : deleteKeys){
			redisson.getBucket(key).delete();
		}

		for (Row row : rows){
			String device_id = row.getString(0);
			String city_code = row.getString(1);
			Date date = new Date();
			RBucket<String> bucket = redisson.getBucket("heartbeat:"+city_code+":"+device_id);
			JSONObject bk = new JSONObject();
			bk.put("timestamp", date);
			bk.put("count", Constants.FAILOVER_HEART_BEAT_TIMES);
			bucket.set(bk.toJSONString());
			bucket.expire(Constants.HEART_EXPIRE_MINUTES, TimeUnit.MINUTES);
		}
		
	}
	
	public static void main(String[] args){
		DeviceExpire dd = new DeviceExpire();
		SparkSQLBuilder builder = new SparkSQLBuilder("heartbeat_init");
		dd.initExpire(builder, "4401");

		RTopic<String> topic = RedisUtils.getRedissonClient().getTopic("__keyevent@0__:expired", StringCodec.INSTANCE);
		topic.addListener(new MessageListener<String>() {
			@Override
			public void onMessage(String channel, String msg) {
				System.out.println("channel:"+channel+" msg:"+msg);
				
			}
		});
	}

}
