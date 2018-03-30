package com.shankephone.data.visualization.computing.ticket.offline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;


/**
 * 支付渠道分析
 * @author fengql
 * @version 2017年8月22日 下午5:06:09
 */
public class PayTypeProcessor implements Executable{
		
	private final static Logger logger = LoggerFactory.getLogger(PayTypeProcessor.class);
	
	public static void main(String[] args) {
		PayTypeProcessor p = new PayTypeProcessor();
		Map<String, Object> argsMap = new HashMap<String, Object>();
		argsMap.put("startDate", "2018-03-15");
		argsMap.put("lastTimestamp", "2018-03-15 11:00:00");
		p.analysePayment("payType", argsMap);
	}
	
	@Override
	public void execute(Map<String, Object> argsMap) {
		logger.info("======================analyse start===========================");
		analysePayment("payType", argsMap);
		logger.info("======================analyse end===========================");
	}
	
	public void analysePayment(String sqlFile, Map<String, Object> argsMap){
		logger.info("=========================start==========================");
		try {
			String timestamp = argsMap == null ? null : argsMap.get("lastTimestamp") == null ? null : argsMap.get("lastTimestamp").toString();
			//返回long型字符串
			String time = DateUtils.getTimestamp(timestamp);
			if(time == null || "".equals(time)){
				throw new RuntimeException("离线任务-支付渠道，没有截止时间戳！");
			}
			Long end = Long.parseLong(time);
			String startDate = argsMap.get("startDate") == null ? null : argsMap.get("startDate").toString();
			String endDate = argsMap.get("endDate") == null ? null : argsMap.get("endDate").toString();
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("lastTimestamp", end + "");
			params.put("startDate", startDate);
			params.put("endDate", endDate);
			SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
			Dataset<Row> results = builder.executeSQL(sqlFile, params);
			processCity(results);
			builder.getSession().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("=========================end==========================");
	}
	
	private void processCity(Dataset<Row> dataset) {
		List<Row> rows = dataset.collectAsList();
		//结果集按城市计算
		RedissonClient redisson = RedisUtils.getRedissonClient();
		Map<String,Map<String,Map<String,Integer>>> dataMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		for(Row r : rows){
			String city_code = r.getString(0);
			String day = r.getString(1);
			if(city_code == null || "".equals(city_code)){
				/*logger.warn("城市代码为空！错误数据：[city_code:" + city_code + 
						", day:" + day + ", payment_code:" + payment_code = ", ticket_num:" + num
						"]");*/
				logger.warn("城市代码为空：" + r.toString()); 
				continue;
			}
			if(day == null || "".equals(day)){
				logger.warn("日期为空:" + r.toString()); 
				continue;
			
			}
			String payment_code = r.getString(2);
			Integer num = (Double)r.getDouble(3) == null ? 0 : ((Double)r.getDouble(3)).intValue();
			
			//获取城市的所有天的支付数据
			Map<String,Map<String,Integer>> cityMap = dataMap.get(city_code);
			if(cityMap == null){
				cityMap = new HashMap<String,Map<String,Integer>>();
			}
			Map<String,Integer> paymentMap = cityMap.get(day);
			if(paymentMap == null){
				paymentMap = new HashMap<String,Integer>();
			}
			//放入支付数据
			paymentMap.put(payment_code.toLowerCase(),num);
			cityMap.put(day, paymentMap);
			dataMap.put(city_code, cityMap);
		}
		Set<String> keySet = dataMap.keySet();
		for(String cityCode : keySet){
			Map<String,Map<String,Integer>> cityMap = dataMap.get(cityCode);
			Set<String> daySet = cityMap.keySet();
			//缓存名称
			String cacheName = Constants.REDIS_NAMESPACE_TICKET_PAYMENT 
					+ cityCode;
			//清除缓存
			RKeys keys = redisson.getKeys();
			keys.deleteByPattern(cacheName + "*");
			for(String day : daySet){
				Map<String,Integer> dayMap = cityMap.get(day);
				redisson.getMap(cacheName).put(day, dayMap);
				logger.warn("put redis: " + day + "-" + dayMap);
				JSONObject json = JSONObject.parseObject(JSON.toJSONString(dayMap));
				redisson.getTopic(cacheName).publish(json.toJSONString());
				logger.warn("publish redis " + cacheName + ": " + json.toJSONString());
			}
		}
	}
	
}
