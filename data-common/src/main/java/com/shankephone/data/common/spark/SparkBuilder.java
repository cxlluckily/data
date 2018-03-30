package com.shankephone.data.common.spark;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.shankephone.data.common.util.PropertyAccessor;
/**
 * Spark相关构造器
 * @author DuXiaohua
 * @version 2017年9月21日 下午3:00:24
 */
public class SparkBuilder {
	/**
	 * 当前app最终合并之后的配置
	 */		
	protected Properties config = new Properties();
	/**
	 * 配置文件中用来标识此应用的名称
	 */
	protected String appName = "";
	/**
	 * 实际使用的Spark AppName
	 */
	protected String realAppName = "";
	
	public SparkBuilder(Class clazz) {
		this(clazz.getSimpleName());
	}
	
	public SparkBuilder(String appName) {
		this.appName = appName;
		mergeConfig();
		realAppName = config.getProperty("appName");
		if (StringUtils.isBlank(realAppName)) {
			realAppName = appName;
		}
	}
	
	protected void mergeConfig() {
		Properties sparkConfig = PropertyAccessor.getProperties("default.spark");
		Properties appConfig = PropertyAccessor.getProperties(appName + ".spark");
		config.putAll(sparkConfig);
		config.putAll(appConfig);
	}

	public Properties getConfig() {
		return config;
	}

	public void setConfig(Properties config) {
		this.config = config;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getRealAppName() {
		return realAppName;
	}

	public void setRealAppName(String realAppName) {
		this.realAppName = realAppName;
	}
	
}
