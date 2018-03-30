package com.shankephone.data.common.util;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import com.shankephone.data.common.spring.ContextAccessor;
/**
 * 读取properties配置文件工具类。
 * 需在Spring上下文中注册此bean。
 *  * <pre>
 * {@code 
 * <bean id="propertyAccessor" class="com.shankephone.data.common.util.PropertyAccessor">
 *		<property name="fileLocations">
 *			<array>
 *				<value>classpath:application.properties</value>
 *			</array>
 *		</property>
 *	</bean>
 * }
 * </pre>
 * @author DuXiaohua
 * @version 2017年5月8日 下午3:16:02
 */
public class PropertyAccessor implements InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(PropertyAccessor.class);
	private Resource[] locations;
	private static Properties props = new Properties();
	
	public static Properties getProperties() {
		ContextAccessor.getApplicationContext();
		return props;
	}
	/**
	 * 读取配置文件中指定key的值
	 * @author DuXiaohua
	 * @date 2017年5月8日 下午3:19:41
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return getProperties().getProperty(key);
	}
	/**
	 * 读取配置文件中指定key的值，如不存在或者没有值，返回默认值
	 * @author DuXiaohua
	 * @date 2017年5月8日 下午3:20:25
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static String getProperty(String key, String defaultValue) {
		String value = getProperties().getProperty(key);
		return StringUtils.isBlank(value) ? defaultValue : value;
	}
	/**
	 * 读取配置文件中指定前缀的所有值
	 * @author DuXiaohua
	 * @date 2017年5月8日 下午3:20:25
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static Properties getProperties(String keyPrefix) {
		return getProperties(getProperties(), keyPrefix);
	}
	
	/**
	 * 从指定properties中取指定前缀的所有值
	 * @author DuXiaohua
	 * @date 2017年9月21日 下午4:12:48
	 * @param properties
	 * @param keyPrefix
	 * @return
	 */
	public static Properties getProperties(Properties properties, String keyPrefix) {
		if (!keyPrefix.endsWith(".")) {
			keyPrefix = keyPrefix + ".";
		}
		Properties tempProps = new Properties();
		Enumeration<?> en = properties.propertyNames();
		while (en.hasMoreElements()) {
			String strKey = en.nextElement().toString();
			if (strKey.startsWith(keyPrefix)) {
				tempProps.setProperty(StringUtils.substringAfter(strKey, keyPrefix), getProperties().getProperty(strKey));
			}
		}
		return tempProps;
	}
	
	public void setFileLocation(Resource location) throws IOException {
		setFileLocations(new Resource[] { location });
	}

	public void setFileLocations(Resource[] locations) throws IOException {
		this.locations = locations;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		for (Resource res : locations) {
			try {
				props.load(res.getInputStream());
				logger.info("Load properties file :" + res.getURL());
			} catch (IOException ex) {
				logger.error("Could not load properties file :" + res.getURL(), ex);
			}
		}
	}

}
