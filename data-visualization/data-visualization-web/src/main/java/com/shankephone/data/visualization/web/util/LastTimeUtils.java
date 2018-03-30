package com.shankephone.data.visualization.web.util;

import org.apache.commons.lang3.StringUtils;
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
}
