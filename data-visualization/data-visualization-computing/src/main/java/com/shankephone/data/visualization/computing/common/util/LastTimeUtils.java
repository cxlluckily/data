package com.shankephone.data.visualization.computing.common.util;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.redisson.api.RBucket;

import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.DateUtils;

public class LastTimeUtils {
	/**
	 * 获取Redis中lastTime的值，格式化为：yyyy-MM-dd HH:mm:ss，如果未找到则取当前时间
	 * @author duxiaohua
	 * @date 2018年2月8日 下午3:31:59
	 * @return
	 */
	public static String getLastTime() {
		RBucket<String> lastTimeR = RedisUtils.getRedissonClient().getBucket("lastTime");
		String lastTime = lastTimeR.get();
		if (StringUtils.isBlank(lastTime)) {
			lastTime = DateUtils.getCurrentDateTime();
		}
		return lastTime;
	}
	/**
	 * 获取Redis中lastTime的值，格式化为：yyyy-MM-dd，如果未找到则取当前时间。
	 * @author duxiaohua
	 * @date 2018年2月8日 下午3:31:59
	 * @return
	 */
	public static String getLastTimeDate() {
		RBucket<String> lastTimeR = RedisUtils.getRedissonClient().getBucket("lastTime");
		String lastTime = lastTimeR.get();
		if (StringUtils.isBlank(lastTime)) {
			return DateUtils.getCurrentDate();
		}
		return DateUtils.convertDateStr(lastTime, DateUtils.DATETIME_PATTERN_DEFAULT);
	}
	
	/**
	 * 是否发布时间
	 * 大于缓存时间或缓存时间为空时发布
	 * @param redisson
	 * @param cityCode
	 * @param date
	 */
	public static boolean canPublishLastTime(String datetime) {
		String time = LastTimeUtils.getLastTime();
		Date today = new Date();
		String currentDate = DateUtils.formatDate(today);
		String dataDate = DateUtils.formatDate(DateUtils.parseDate(datetime,"yyyy-MM-dd HH:mm:ss")); 
		//如果数据日期与当前服务器日期是同一天
		if(dataDate.equals(currentDate)){
			//如果数据时间大于缓存时间，则更新缓存时间并发布数据
			if(datetime.compareTo(time) > 0){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 是否发布数据
	 * 数据日期大于等于当天时发布数据
	 * @param redisson
	 * @param day
	 * @return
	 */
	public static boolean canPublishData(String day) {
		if(day != null && !"".equals(day)){
			String date = LastTimeUtils.getLastTimeDate();
			if(date != null && !"".equals(date)){
				//如果数据日期等于缓存日期，则发布数据
				if(day.equals(date)){
					return true;
				}
				return false;
			} 
		}
		return false;
	}
}
