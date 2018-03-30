package com.shankephone.data.visualization.web.ticket;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.visualization.web.util.LastTimeUtils;

/**
 * 近30日售票量初始化
 * @author 森
 * @version 2017年9月15日 下午4:21:00
 */

public class DayTicketsInitProcessor implements SubDataInitProcessor {
	public final static Logger logger = LoggerFactory.getLogger(DayTicketsInitProcessor.class); 
	@Override
	public Object process(SubInfo subInfo) {
		String topics = subInfo.getTopic();
		String city_code = topics.split(":")[2];

		JSONObject day_tickets = new JSONObject();
		List<String> time = new ArrayList<String>();
		List<Integer> tickets = new ArrayList<Integer>();
				
		RMap<String,Map<String,Integer>> cacheMap = RedisUtils.getRedissonClient().getMap("trade:vol:" + city_code);
		if(cacheMap==null){
			return day_tickets;
		}
		Calendar calendar = Calendar.getInstance();
		//获取redis缓存的数据日期
		String last_date = LastTimeUtils.getLastTimeDate();
		if(last_date == null || "".equals(last_date)){
			return new JSONObject();
		}
		calendar.setTime(DateUtils.parseDate(last_date));
		calendar.add(Calendar.DATE, -30);
		String before = DateUtils.formatDate(calendar.getTime());
		int i = 0;
		while (i<30) {
			if(cacheMap.get(before)!=null){
				tickets.add(cacheMap.get(before).get("all"));
				time.add(before);
			}else{
				tickets.add(0);
				time.add(before);
			}
			calendar.add(Calendar.DATE, 1);
			before = DateUtils.formatDate(calendar.getTime());
			i++;
		}
		day_tickets.put("TIME", time);
		day_tickets.put("TICKETS", tickets);
		logger.info("【day:tickets:"+city_code+"】"+day_tickets);
		return day_tickets;
	}

}
