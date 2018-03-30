package com.shankephone.data.cache;
/**
 * 设备状态字典值缓存
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:28:08
 */
public class DeviceStatusDicCache extends AbstractMapCache<DeviceStatusDic> {
	
	private static final DeviceStatusDicCache CACHE = new DeviceStatusDicCache(DeviceStatusDic.class, "SKP:DEVICE_STATUS_DIC", "device_status_dic", "statusId", "statusValue");

	public DeviceStatusDicCache(Class<DeviceStatusDic> clazz, String hbaseTable, String redisKey, String... mapKeyPropertyName) {
		super(clazz, hbaseTable, redisKey, mapKeyPropertyName);
	}
	
	public static DeviceStatusDicCache getInstance() {
		return CACHE;
	}
	
}
