package com.shankephone.data.visualization.computing.user.streaming;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RKeys;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

/**
 * 当日用户数-实时计算
 * @author fengql
 * @version 2018年3月12日 下午4:01:05
 */
public class UserCountProcessor implements SparkStreamingProcessor {

	@Override
	public void process(JavaStreamingContext context, JavaRDD<JSONObject> rdd,
			Map<String, Object> args) {
		String redisKey = Constants.REDIS_USER_ID_SET;
		rdd.filter(value -> {
			String tableName = value.getString("tableName");
			JSONObject columns = value.getJSONObject("columns");
			if ("SKP:ORDER_INFO".equals(tableName)) {
				String cityCode = columns.getString("CITY_CODE");
				String payTime = columns.getString("PAY_PAY_TIME");
				String payState = columns.getString("PAY_STATE");
				String payAccount = columns.getString("PAY_PAY_ACCOUNT");
				boolean cityCodeFlag = cityCode != null && !"".equals(cityCode);
				boolean payTimeFlag = payTime != null && !"".equals(payTime);
				boolean payStateFlag = payState != null && "2".equals(payState);
				boolean payAccountFlag = payAccount != null && !"".equals(payAccount);
				if(cityCodeFlag && payTimeFlag && payStateFlag && payAccountFlag){
					return true;
				}
				return false;
			}
			return false;
		}).mapToPair(value -> {
			JSONObject columns = value.getJSONObject("columns");
			String cityCode = columns.getString("CITY_CODE");
			String payTime = columns.getString("PAY_PAY_TIME");
			String payAccount = columns.getString("PAY_PAY_ACCOUNT");
			String day = payTime.split(" ")[0];
			String key = cityCode + ":" + day;
			Tuple2<String,String> tuple = new Tuple2<String,String>(key, payAccount);
			return tuple;
		}).foreachPartition(f -> {
			RedissonClient redis = RedisUtils.getRedissonClient();
			while(f.hasNext()) {
				Tuple2<String,String> tuple = f.next();
				String key = tuple._1;
				String account = tuple._2;
				RSet<String> set = redis.getSet(redisKey + key);
				set.add(account);
			}
		});
		
		//实时发布当日用户数
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RKeys rkeys = redisson.getKeys();
		Iterable<String> its = rkeys.getKeysByPattern(redisKey + "*");
		Iterator<String> it = its.iterator();
		while(it.hasNext()){
			String keystr = it.next();
			String [] keys = keystr.split(":");
			String cityCode = keys[2];
			String day = keys[3];
			RSet<String> set = redisson.getSet(keystr);
			int count = set.size();
			String date = LastTimeUtils.getLastTimeDate();
			if(day.equals(date)){
				JSONObject json = new JSONObject();
				json.put("USER_COUNT", count);
				redisson.getTopic(redisKey + cityCode).publish(json.toJSONString());
			}
		}
		
	}
	
	/*public static void main(String[] args) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		//实时发布当日用户数
		RKeys rkeys = redisson.getKeys();
		Iterable<String> its = rkeys.getKeysByPattern("user:uniq:*");
		Iterator<String> it = its.iterator();
		while(it.hasNext()){
			String keystr = it.next();
			String [] keys = keystr.split(":");
			String cityCode = keys[2];
			String day = keys[3];
			RSet<String> set = redisson.getSet(keystr);
			int count = set.size();
			String date = LastTimeUtils.getLastTimeDate();
			if(day.equals(date)){
				System.out.println(cityCode + "  " + day + "  " + count);
			}
		}
	}*/

}
