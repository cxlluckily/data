package com.shankephone.data.common.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;
/**
 * <p>提供对Spring上下文的一些常用快捷访问方法。</p>
 * <p>要使用此类，需在spring配置文件中做如下配置：</p>
 * <p>
 * <pre>
 * {@code 
 * <bean id="contextAccessor" class="com.shankephone.data.common.util.ContextAccessor"/>
 * }
 * </pre>
 * </p>
 * @author DuXiaohua
 * @version 2014-7-16 下午10:47:00
 * @see ApplicationContext
 */
public class ContextAccessor implements ApplicationContextAware {
	
	private static volatile ApplicationContext applicationContext;
	
	/**
	 * 获取Spring上下文对象，如果不存在则初始化
	 * @return ApplicationContext
	 */
	public static ApplicationContext getApplicationContext() {
		if (applicationContext == null) {
			init();
		}
		return applicationContext;
	}
	
	/**
	 * @see ApplicationContext#getBean(String)
	 */
	public static Object getBean(String name) {
		return getApplicationContext().getBean(name);
	}
	
	/**
	 * @see ApplicationContext#getBean(String, Class)
	 */
	public static <T> T getBean(String name, Class<T> requiredType) {
		return getApplicationContext().getBean(name, requiredType);
	}
	
	/**
	 * @see ApplicationContext#getBean(Class)
	 */
	public static <T> T getBean(Class<T> requiredType) {
		return getApplicationContext().getBean(requiredType);
	}
	
	/**
	 * @see ApplicationContext#getBean(String, Object...)
	 */
	public static Object getBean(String name, Object... args) {
		return getApplicationContext().getBean(name, args);
	}
	
	/**
	 * @see ApplicationContext#containsBean(String)
	 */
	public static boolean containsBean(String name) {
		return getApplicationContext().containsBean(name);
	}
	
	/**
	 * @see ApplicationContext#isSingleton(String)
	 */
	public static boolean isSingleton(String name) {
		return getApplicationContext().isSingleton(name);
	}
	
	/**
	 * @see ApplicationContext#isPrototype(String)
	 */
	public static boolean isPrototype(String name) {
		return getApplicationContext().isPrototype(name);
	}
	
	/**
	 * @see ApplicationContext#isTypeMatch(String, Class)
	 */
	public static boolean isTypeMatch(String name, Class<?> targetType) {
		return getApplicationContext().isTypeMatch(name, targetType);
	}
	
	/**
	 * @see ApplicationContext#getType(String)
	 */
	public static Class<?> getType(String name) {
		return getApplicationContext().getType(name);
	}
	
	/**
	 * @see ApplicationContext#getAliases(String)
	 */
	public static String[] getAliases(String name) {
		return getApplicationContext().getAliases(name);
	}
	
	public void setApplicationContext(ApplicationContext ac) {
		applicationContext = ac;
	}
	/**
	 * 初始化spring上下文，此方法是线程安全的。
	 * @author DuXiaohua
	 * @date 2017年8月4日 下午2:27:03
	 */
	private static synchronized void init() {
		if (applicationContext == null) {
			applicationContext = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
		}
	}
}
