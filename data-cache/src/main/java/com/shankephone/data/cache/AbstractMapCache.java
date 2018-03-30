package com.shankephone.data.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import com.shankephone.data.common.hbase.HBaseClient;
import com.shankephone.data.common.redis.RedisUtils;
/**
 * Redis Hash结构缓存支持类
 * 1.提供基本的从HBase加载数据到Redis Hash
 * 2.提供对Redis基本的查询方法，在第一次查询时将Redis数据缓存到本地，后续直接从本地查询
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:16:12
 * @param <T>
 */
public abstract class AbstractMapCache<T> {
	/**
	 * 本地缓存
	 */
	protected Map<String, T> cacheMap;
	/**
	 * 缓存数据对应的类
	 */
	protected Class<T> clazz;
	/**
	 * HBase中对应的表
	 */
	protected String hbaseTable;
	/**
	 * Redis中此Hash结构对应的Key
	 */
	protected String redisKey;
	/**
	 * Redis Hash结构中field的组成字段，对应实体类中的属性，Redis中按下划线分隔
	 */
	protected String[] mapKeyPropertyName;
	
	public AbstractMapCache(Class<T> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		this.clazz = clazz;
		this.hbaseTable = hbaseTable;
		this.redisKey = redisKey;
		this.mapKeyPropertyName = mapKeyPropertyName;
	}
	/**
	 * 获取指定key的缓存对象，key为可变长度参数，支持多个参数。底层会将多个参数按下划线拼接为一个字符串进行查询。
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:23:59
	 * @param key
	 * @return
	 */
	public T get(String... key) {
		return getCacheMap().get(String.join("_", key));
	}
	/**
	 * 获取缓存中的所有对象
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:26:13
	 * @return
	 */
	public Collection<T> getAll() {
		return getCacheMap().values();
	}
	
	public Map<String, T> getCacheMap() {
		if (cacheMap == null) {
			initLocal();
		}
		return cacheMap;
	}
	
	protected synchronized void initLocal() {
		if (cacheMap == null) {
			refreshLocal();
		}
	}
	
	public synchronized void refreshLocal() {
		RedissonClient cacheClient = RedisUtils.getCacheRedissonClient();
		RMap<String, T> cacheMapRedis = cacheClient.getMap(redisKey);
		cacheMap = cacheMapRedis.readAllMap();
		afterRefreshLocal();
	}
	/**
	 * 初始化或者刷新本地缓存之后进行自定义扩展处理
	 * @author DuXiaohua
	 * @date 2017年11月9日 下午5:06:51
	 */
	protected void afterRefreshLocal() {
		
	}
	/**
	 * 从HBase中初始化数据到Redis
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:26:44
	 */
	public synchronized void initRedis() {
		List<T> resultList = HBaseClient.getInstance().getAll(hbaseTable, clazz);
		fillObject(resultList);
		RedissonClient cacheClient = RedisUtils.getCacheRedissonClient();
		RMap<Object, T> cacheMap = cacheClient.getMap(redisKey);
		cacheMap.delete();
		for (T obj : resultList) {
			cacheMap.fastPut(getMapKeyPropertyValue(obj), obj);
		}
	}
	/**
	 * 在将HBase中的数据放入Redis之前对其进行自定义的填充处理
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:27:06
	 * @param objList
	 */
	protected void fillObject(List<T> objList) {
		
	}
	
	private String getMapKeyPropertyValue(T obj) {
		String[] value = new String[mapKeyPropertyName.length];
		for (int i = 0; i < mapKeyPropertyName.length; i++) {
			try {
				value[i] = PropertyUtils.getProperty(obj, mapKeyPropertyName[i]).toString();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return String.join("_", value);
	}
	
}
