package com.shankephone.data.visualization.computing.user.offline;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RKeys;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

/**
 * 当日用户数-离线统计
 * 用户数取自城市-天的set的数量
 * @author fengql
 * @version 2018年3月12日 下午3:40:13
 */
public class UserCountProcessor implements Executable {

	private final static Logger logger = LoggerFactory.getLogger(UserCountProcessor.class);
	
	@Override
	public void execute(Map<String, Object> argsMap) {
		logger.info("=========================start==========================");
		Map<String,Object> params = new HashMap<String,Object>();
		
		String t_last_timestamp = argsMap == null ? null : argsMap.get("lastTimestamp") == null ? null : argsMap.get("lastTimestamp").toString();
		Date date = DateUtils.parseDate(t_last_timestamp, "yyyy-MM-dd HH:mm:ss");
		argsMap.put("lastTimestamp", date.getTime() + "");
		params.putAll(argsMap);
		SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
		Dataset<Row> results = builder.executeSQL("userCount", params);
		
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String redisKey = Constants.REDIS_USER_ID_SET;
		//先清除redis中的set
		RKeys rkeys = redisson.getKeys();
		rkeys.deleteByPattern(redisKey + "*");
		results.foreachPartition(f -> {
			RedissonClient redis = RedisUtils.getRedissonClient();
			while(f.hasNext()){
				Row row = f.next();
				String city_code = row.getString(0);
				String day = row.getString(1);
				String account = row.getString(2);
				RSet<String> set = redis.getSet(redisKey + city_code + ":" + day);
				set.add(account);
			}
		});
		
		Iterable<String> keys = rkeys.getKeysByPattern(redisKey + "*");
		//按【城市-当前日期】缓存和发布用户数:遍历所有key，找到与lastTimestamp是同一天的数据进行发布
		for(String key : keys){
			String keyarr [] = key.split(":");
			String cityCode = keyarr[2];
			String dateKey = keyarr[3];
			//获取当前城市当日的用户数
			String day = LastTimeUtils.getLastTimeDate();
			if(day.equals(dateKey)){
				RSet<String> set = redisson.getSet(redisKey + cityCode + ":" + dateKey);
				int count = set.size();
				JSONObject json = new JSONObject();
				json.put("USER_COUNT", count);
				//发布当前城市当日的用户数
				redisson.getTopic(redisKey + cityCode).publish(json.toJSONString());
			}
		}
		builder.getSession().close();
		logger.info("=========================end==========================");
		
	}
	
	public static void main(String[] args) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String redisKey = Constants.REDIS_USER_ID_SET;
		RKeys rkeys = redisson.getKeys();
		Iterable<String> keys = rkeys.getKeysByPattern(redisKey + "*");
		//按【城市-当前日期】缓存和发布用户数
		for(String key : keys){
			System.out.println(key);
		}
	}

}
