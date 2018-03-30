package com.shankephone.data.common.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
/**
 * 日期相关工具类
 * @author duxiaohua
 * @version 2018年2月8日 下午2:54:31
 */
public class DateUtils {
	
	public static final String DATE_PATTERN_DEFAULT = "yyyy-MM-dd";
	public static final String DATETIME_PATTERN_DEFAULT = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 获取当前时间字符串，格式：yyyy-MM-dd
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String getCurrentDate() {
		return getCurrentDate(DATE_PATTERN_DEFAULT);
	}
	/**
	 * 获取当前时间字符串，格式：yyyy-MM-dd HH:mm:ss
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String getCurrentDateTime() {
		return getCurrentDate(DATETIME_PATTERN_DEFAULT);
	}
	/**
	 * 获取当前时间字符串，格式化为指定格式
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String getCurrentDate(String pattern) {
		Date date = new Date();
		return formatDate(date, pattern);
	}
	/**
	 * 将指定日期格式化为指定格式
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String formatDate(Date date, String pattern) {
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}
	/**
	 * 将指定日期格式化为：yyyy-MM-dd
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String formatDate(Date date) {
		return formatDate(date, DATE_PATTERN_DEFAULT);
	}
	/**
	 * 将指定日期格式化为：yyyy-MM-dd HH:mm:ss
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String formatDateTime(Date date) {
		return formatDate(date, DATETIME_PATTERN_DEFAULT);
	}
	/**
	 * 按指定格式将字符串解析为日期格式
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final Date parseDate(String dateStr, String pattern) {
		DateFormat df = new SimpleDateFormat(pattern);
		try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	/**
	 * 按格式:yyyy-MM-dd，将字符串解析为日期格式
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final Date parseDate(String dateStr) {
		return parseDate(dateStr, DATE_PATTERN_DEFAULT);
	}
	/**
	 * 按格式:yyyy-MM-dd HH:mm:ss，将字符串解析为日期格式
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final Date parseDateTime(String dateStr) {
		return parseDate(dateStr, DATETIME_PATTERN_DEFAULT);
	}
	/**
	 * 将日期字符串，转换为指定格式。需指定日期字符串原格式originPattern和目标格式targetPattern。
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String convertDateStr(String dateStr, String originPattern, String targetPattern) {
		Date date = parseDate(dateStr, originPattern);
		return formatDate(date, targetPattern);
	}
	/**
	 * 将日期字符串，转换为默认格式：yyyy-MM-dd。需指定日期字符串原格式originPattern。
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String convertDateStr(String dateStr, String originPattern) {
		return convertDateStr(dateStr, originPattern, DATE_PATTERN_DEFAULT);
	}
	/**
	 * 将日期字符串，转换为默认格式：yyyy-MM-dd HH:mm:ss。需指定日期字符串原格式originPattern。
	 * @author duxiaohua
	 * @date 2018年2月8日 下午2:47:15
	 * @return
	 */
	public static final String convertDateTimeStr(String dateStr, String originPattern) {
		return convertDateStr(dateStr, originPattern, DATETIME_PATTERN_DEFAULT);
	}
	
	public static String getTimestamp(String datetime){
		if(datetime == null || "".equals(datetime)){
			return null;
		}
		Date date = parseDate(datetime, "yyyy-MM-dd HH:mm:ss");
		return date.getTime() + "";
	}
	
	public static String getLastDateTerminal(String timestamp, Integer beforeNums){
		String datetime = getTimestamp(timestamp);
		if(datetime == null || "".equals(datetime)){
			Date date = new Date();
			datetime = date.getTime() + "";
		}
		if(beforeNums != null){
			long time = Long.parseLong(datetime);
			Date end = new Date(time);
			Calendar cal = Calendar.getInstance();
			cal.setTime(end);
			cal.add(Calendar.DAY_OF_YEAR, -beforeNums);
			cal.set(Calendar.HOUR_OF_DAY, 23);
			cal.set(Calendar.MINUTE, 59);
			cal.set(Calendar.SECOND, 59);
			time = cal.getTimeInMillis();
			datetime = time + "";
		}
		return datetime;
	}
	
}	
