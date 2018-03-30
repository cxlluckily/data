package com.shankephone.data.monitoring.computing.device;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.monitoring.computing.device.model.FailureDetails;
import com.shankephone.data.monitoring.computing.device.model.FailureInfo;
import com.shankephone.data.monitoring.computing.device.service.FailureDetailsService;
import com.shankephone.data.monitoring.computing.device.service.FailureInfoService;

public class TestFailureInfo {
	
	@Test
	public void testInsert(){
		FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
		//FailureDetailsService failureDetailsService = (FailureDetailsService)ContextAccessor.getBean("failureDetailsService");
		FailureInfo fi = null;
		//failureInfoService.queryById(deviceId);
		fi = new FailureInfo();
		fi.setCity_code("4401");
		fi.setLine_code("12");
		fi.setStation_code("3122");
		fi.setDevice_id("3306");
		fi.setCreate_time(new Date());
		fi.setStatus_value("010");
		fi.setFailure_time(new Date());
		failureInfoService.insert(fi);
	}
	
	@Test
	public void testDelete(){
		FailureInfoService failureInfoService = (FailureInfoService)ContextAccessor.getBean("failureInfoService");
		failureInfoService.deleteByDeviceId("4401","3306", "1");
	}
	
	@Test
	public void testDetails(){
		FailureDetailsService failureDetailsService = (FailureDetailsService)ContextAccessor.getBean("failureDetailsService");
		FailureDetails fi = null;
		//failureInfoService.queryById(deviceId);
		fi = new FailureDetails();
		fi.setCity_code("4401");
		fi.setLine_code("12");
		fi.setStation_code("3122");
		fi.setDevice_id("3306");
		fi.setCreate_time(new Date());
		fi.setStatus_value("010");
		fi.setFailure_time(new Date());
		failureDetailsService.insert(fi);
	}
	
	@Test
	public void testFormat(){
		String str = "179128587-07-17 16:20:03";
		SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			System.out.println(sdft.parse(str));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSort(){
		List<Integer> list = null;
		Integer [] array = new Integer[]{1,2,3,4,1};
		list = Arrays.asList(array);
		Collections.sort(list, (o1, o2) -> {
			Integer j1 = (Integer)o1;
			Integer j2 = (Integer)o2;
			if(j1 > j2){
				return 1;
			} 
			if(j1 < j2){
				return -1;
			}
			return 0;
		});
		System.out.println(list);
	}

}
