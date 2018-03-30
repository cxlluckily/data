package com.shankephone.data.computing.redis;

import java.io.IOException;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;

public class RedisBaseTest {
	
	@Test
	public void testNamespace(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String,String> rm = redisson.getMap("user:tmp");
		rm.put("4100", "{name:'长沙', app:100, ali:200}");
	}
	
	@Test
	public void testSave(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String,String> rm = redisson.getMap("user-analyse");
		rm.put("4100", "{name:'长沙', app:100, ali:200}");
	}
	
	@Test
	public void testRead(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String,String> rm = redisson.getMap("user-analyse");
		System.out.println(rm.get("4100"));
	}
	
	@Test
	public void testDel(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RKeys keys = redisson.getKeys();
		/*Iterable<String> foundedKeys = keys.getKeysByPattern("index-*");
		long numOfDeletedKeys = keys.delete("obj1", "obj2", "obj3");*/
		long count = keys.deleteByPattern("user:offline:architecture:user-architecture_2017-09-25");
		System.out.println(count);
	}
	
	@Test
	public void testGetKey(){
		/*paymentMap.put("0","支付宝支付");
		paymentMap.put("1","微信支付");
		paymentMap.put("2","中移动支付");
		paymentMap.put("3","翼支付");
		paymentMap.put("4","首信易支付");
		paymentMap.put("5","银联支付");
		paymentMap.put("9","盘缠支付");
		paymentMap.put("99","其它");*/
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String,String> rm = redisson.getMap("ticket:offline:payment:ticket-payment_2017-09-04");
		rm.put("5300","{names:['支付宝支付','微信支付','中移动支付','翼支付','首信易支付','银联支付','其它'],"
				+ "rows:[{code:'0',value:'200',name:'支付宝支付'},"
				+ "{code:'1',value:'330',name:'微信支付'},"
				+ "{code:'2',value:'30',name:'中移动支付'},"
				+ "{code:'3',value:'20',name:'翼支付'},"
				+ "{code:'4',value:'1',name:'首信易支付'},"
				+ "{code:'5',value:'6',name:'银联支付'},"
				+ "{code:'9',value:'100',name:'盘缠支付'},"
				+ "{code:'99',value:'100',name:'其它'}"
				+ "]}");
		rm.put("4100","{names:['支付宝支付','微信支付','中移动支付','翼支付','首信易支付','银联支付','其它'],"
				+ "rows:[{code:'0',value:'23900',name:'支付宝支付'},"
				+ "{code:'1',value:'59021',name:'微信支付'},"
				+ "{code:'2',value:'3000',name:'中移动支付'},"
				+ "{code:'3',value:'210',name:'翼支付'},"
				+ "{code:'4',value:'120',name:'首信易支付'},"
				+ "{code:'5',value:'61',name:'银联支付'},"
				+ "{code:'9',value:'3020',name:'盘缠支付'},"
				+ "{code:'99',value:'2113',name:'其它'}"
				+ "]}");
		rm.put("all","{names:['支付宝支付','微信支付','中移动支付','翼支付','首信易支付','银联支付','其它'],"
				+ "rows:[{code:'0',value:'400',name:'支付宝支付'},"
				+ "{code:'1',value:'320',name:'微信支付'},"
				+ "{code:'2',value:'10',name:'中移动支付'},"
				+ "{code:'3',value:'50',name:'翼支付'},"
				+ "{code:'4',value:'10',name:'首信易支付'},"
				+ "{code:'5',value:'2',name:'银联支付'},"
				+ "{code:'9',value:'300',name:'盘缠支付'},"
				+ "{code:'99',value:'400',name:'其它'}"
				+ "]}");
		System.out.println();
	}
	
	@Test
	public void testBucket(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		System.out.println(bucket.get());
	}

	@Test
	public void testPush(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		JSONObject json = new JSONObject();
		JSONArray names = new JSONArray();
		names.add("微信");
		names.add("支付宝");
		names.add("其它");
		
		JSONArray values = new JSONArray();
		JSONObject j = new JSONObject();
		j.put("name", "微信");
		j.put("value", 0);
		values.add(j);
		JSONObject j1 = new JSONObject();
		j1.put("name", "支付宝");
		j1.put("value", 0);
		values.add(j1);
		JSONObject j2 = new JSONObject();
		j2.put("name", "其它");
		j2.put("value", 0);
		values.add(j2);
		
		json.put("names", names);
		json.put("rows", values);
		
		redisson.getTopic("ticket-payment_4401").publish(json.toJSONString());
	}
	
	@Test
	public void testSub() throws IOException {
		RTopic<String> testTopic = RedisUtils.getRedissonClient().getTopic("test-order-topic");
		testTopic.addListener((channel, msg) -> {
			System.out.println("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	
	public static void main(String[] args) throws IOException {
		RTopic<String> testTopic = RedisUtils.getRedissonClient().getTopic("pay:type:4401");
		testTopic.addListener((channel, msg) -> {
			System.out.println("pay:type:4401" + ",msg:" + msg);
		});
		System.in.read();
	}
}
