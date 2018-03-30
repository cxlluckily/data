package com.shankephone.data.visualization.computing.ticket.offline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.redisson.api.RMap;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;
import com.shankephone.data.visualization.computing.ticket.offline.vo.GateTransitCount;
/**
 * 云闸机过闸统计（离线）
 * @author duxiaohua
 * @version 2018年3月12日 下午11:37:22
 */
public class GateTransitCountProcessor implements Executable {
	
	@Override
	public void execute(Map<String, Object> args) {
		SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
		Dataset<Row> dataset = builder.executeSQL("gateTransitCount", args);
		Dataset<GateTransitCount> datasetCount = dataset.map(row -> {
			String tranDate = row.getString(0);
			String productCategory = row.getString(1);
			Integer total = ((Long)row.getLong(2)).intValue();
			GateTransitCount gateTransitCount = new GateTransitCount();
			gateTransitCount.setTranDate(DateUtils.convertDateStr(tranDate, "yyyyMMdd"));
			gateTransitCount.setProductCategory(Constants.getGateTransitTypeName(productCategory));
			gateTransitCount.setTotal(total);
			return gateTransitCount;
		}, Encoders.bean(GateTransitCount.class));
		List<GateTransitCount> countList = datasetCount.collectAsList();
		Map<String, Map<String, Integer>> resultMap = new HashMap<>();
		for (GateTransitCount gateTransitCount : countList) {
			String tranDate = gateTransitCount.getTranDate();
			String productCategory = gateTransitCount.getProductCategory();
			Integer total = gateTransitCount.getTotal();
			Map<String, Integer> tranDateMap = resultMap.get(tranDate);
			if (tranDateMap == null) {
				tranDateMap = new HashMap<String, Integer>();
				resultMap.put(tranDate, tranDateMap);
			}
			Integer allCount = tranDateMap.get("all");
			if (allCount == null) {
				allCount = 0;
			}
			allCount = allCount + total;
			tranDateMap.put("all", allCount);
			if (productCategory.equalsIgnoreCase("other")) {
				Integer otherCount = tranDateMap.get("other");
				if (otherCount != null) {
					total = total + otherCount;
				}
			}
			tranDateMap.put(productCategory, total);
		}
		RMap<String, Map<String, Integer>> rmap = RedisUtils.getRedissonClient().getMap(Constants.GATETRANSIT_REDIS_KEY);
		rmap.putAll(resultMap);
		String lastTime = LastTimeUtils.getLastTimeDate();
		Map<String, Integer> lastMap = resultMap.get(lastTime);
		if (lastMap != null) {
			RedisUtils.getRedissonClient().getTopic(Constants.GATETRANSIT_REDIS_KEY).publish(JSONObject.toJSONString(lastMap));
		}
		builder.getSession().close();
	}

}
