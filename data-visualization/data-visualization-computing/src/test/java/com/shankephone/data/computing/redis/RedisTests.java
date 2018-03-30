package com.shankephone.data.computing.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RTopic;
import org.redisson.client.protocol.ScoredEntry;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.test.SpringTestCase;

public class RedisTests extends SpringTestCase {
	
	@Test
	public void testPub() throws InterruptedException {
		RTopic<String> testTopic = RedisUtils.getRedissonClient().getTopic("trade:vol:0000");
		while (true) {
			testTopic.publish("hello word!" + "[" + System.currentTimeMillis() + "]");
			Thread.sleep(1000);
		}
	}
	
	@Test
	public void testSub() throws IOException {
		RTopic<String> testTopic = RedisUtils.getRedissonClient().getTopic("trade:vol:4401");
		testTopic.addListener((channel, msg) -> {
			logger.info("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	@Test
	public void testSetBucket(){
		RBucket<String> bucket = RedisUtils.getRedissonClient().getBucket("anyObject");
		bucket.set("test");
	}
	
	@Test
	public void testGetBucket(){
		RBucket<String> bucket = RedisUtils.getRedissonClient().getBucket("cityLineStationRelations");
		System.out.println(bucket.isExists());
//		System.out.println(bucket.get());
	}
	
	
	@Test
	public void testSubRLTICKET() throws IOException {
		RTopic<String> RTTICKET = RedisUtils.getRedissonClient().getTopic("RT-TICKET");
		RTTICKET.addListener((channel, msg) -> {
			logger.info("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	@Test
	public void testHSTOP10() throws IOException {
		RTopic<String> HSTOP10 = RedisUtils.getRedissonClient().getTopic("HS-TOP10");
		HSTOP10.addListener((channel, msg) -> {
			logger.info("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	@Test
	public void testHSTOP() throws IOException {
		JSONArray currentTop10 = new JSONArray();
		RScoredSortedSet<String> set = RedisUtils.getRedissonClient().getScoredSortedSet("hstop10-4100_2017-08-30");
		Collection<ScoredEntry<String>> entryRangeReversed = set.entryRangeReversed(0, set.size());
		
		int i=1;
		for(ScoredEntry<String> t : entryRangeReversed){
			JSONObject rank = new JSONObject();
			rank.put("RANK", "NO"+i);
			i++;
			rank.put("TICKET_COUNT", t.getScore().longValue());
			rank.put("STATION_NAME", "长沙");
			currentTop10.add(rank);
		}
		//TODO 待处理，目前存入redis
		RedisUtils.getRedissonClient()
        .getTopic("hstop10-4100")
        .publish(currentTop10.toJSONString());
	}
	
	@Test
	public void testCountExists() throws IOException {
		RKeys keys = RedisUtils.getRedissonClient().getKeys();
//		System.out.println(keys.countExists("hstop10-2660_2017-08-29"));
		keys.delete("hstop10-2660_2017-08-26");
//		if(keys.countExists(hot_station_topic+"_"+currentDay)<=0){
//			keys.delete(hot_station_topic+"_"+preDay);
//		}
	}
	
	
	@Test
	public void testSets() throws IOException {
		RSet<Object> set = RedisUtils.getRedissonClient().getSet("RSetTest");
		set.add("1111");
		set.add("2222");
		set.add("1111");
	
	}
	
	@Test
	public void generatePeopleStream() throws IOException {
		List<String> data = new ArrayList<String>();
		data.add("新城东");
		data.add("桂城");
		data.add("西朗");
		data.add("文化公园");
		data.add("西门口");
		data.add("广州火车站");
		data.add("同和");
		data.add("植物园");
		data.add("黄村");
		data.add("东圃");
		data.add("大学城北");
		data.add("南村万博");
		int index=1;
		while(true){
			
			try {
				Thread.sleep(1000);
				Random random1 = new Random(600+index);
				Random random2 = new Random(300+index);
				Random random3 = new Random(index);
				JSONArray links = new JSONArray();
				for(int i=0,len=random3.nextInt(10)+1;i<len;i++){
					int source  = random1.nextInt(data.size());
					int target = random2.nextInt(data.size());
					int value = random3.nextInt(20);
					if(source != target){
						JSONObject tmp = new JSONObject();
						tmp.put("source", data.get(source));
						tmp.put("target", data.get(target));
						tmp.put("value",value);
						links.add(tmp);
					}
				}
				System.out.println(links.toJSONString());
				RedisUtils.getRedissonClient()
		        .getTopic("PEOPLE-STREAM")
		        .publish(links.toJSONString());
				index++;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	@Test
	public void generateTop10() throws IOException {
		while(true){
			try {
				Thread.sleep(2000);
				JSONObject links = new JSONObject();
				links.put("RANK", "2222");
				links.put("STATION_CODE", "2222");
				links.put("VALUE", "2222");
				System.out.println(links.toJSONString());
				RedisUtils.getRedissonClient()
		        .getTopic("HS-TOP10")
		        .publish(links.toJSONString());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	@Test
	public void generateTickets() throws IOException {
		while(true){	
			try {
				Thread.sleep(2000);
				JSONObject links = new JSONObject();
				links.put("CITY_CODE", "2222");
				links.put("TICKET_PICKUP_LINE_NAME_CN", "2222");
				links.put("TICKET_GETOFF_LINE_NAME_CN", "2222");
				links.put("TICKET_ACTUAL_TAKE_TICKET_NUM", "2222");
				System.out.println(links.toJSONString());
				RedisUtils.getRedissonClient()
		        .getTopic("RT-TICKET")
		        .publish(links.toJSONString());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	@Test
	public void getTicketsCount() throws IOException {
		RScoredSortedSet<String> set = RedisUtils.getRedissonClient().getScoredSortedSet("CityTicketPriceRank");
		Collection<ScoredEntry<String>> entryRangeReversed = set.entryRange(0, set.size());
		for(ScoredEntry<String> entry : entryRangeReversed){
			System.out.println(entry.getValue()+" "+entry.getScore().longValue());
		}
	}
}
