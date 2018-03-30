package com.shankephone.data.monitoring.computing.device;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.shankephone.data.common.spring.ContextAccessor;

public class TestPhoenixWriter {
	
	public static void main(String[] args) {
		
		
		/*FailureInfoPhoenixService failureInfoPhoenixService = (FailureInfoPhoenixService)ContextAccessor.getBean("failureInfoPhoenixService");
		FailureInfoPhoenix failInfo = new FailureInfoPhoenix();
		//BeanUtils.copyProperties(fi, failInfo);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		failInfo.setCITY_CODE("4401");
		failInfo.setCITY_NAME("广州");
		failInfo.setCREATE_TIME(sdf.format(new Date()));
		failInfo.setDEVICE_ID("0509045335s");
		failInfo.setDEVICE_NAME("区庄5335");
		failInfo.setDEVICE_TYPE("02");
		failInfo.setDEVICE_TYPE_NAME("闸机");
		failInfo.setFAILURE_TIME("2017-11-03 11:46:09");
		failInfo.setLINE_CODE("05");
		failInfo.setLINE_NAME("5号线");
		failInfo.setSTATION_CODE("0509");
		failInfo.setSTATION_NAME("区庄");
		failInfo.setSTATUS_VALUE("1");
		failInfo.setFAILURE_TYPE("1");
		failInfo.setRECOVER_TIME(sdf.format(new Date()));*/
		
		/*System.out.println(failInfo.getCITY_CODE() + "---" +failInfo.getDEVICE_ID() + "---" +failInfo.getFAILURE_TYPE());
		String PK = failureInfoPhoenixService.selectPK(failInfo.getCITY_CODE(), failInfo.getDEVICE_ID(), failInfo.getFAILURE_TYPE());
		if(PK == null){
			PK = failInfo.getCITY_CODE() +"_"+failInfo.getDEVICE_ID()+"_"+failInfo.getFAILURE_TIME()+"_"+failInfo.getFAILURE_TYPE();
		}*/
		//String PK = failureInfoPhoenixService.selectPK(failInfo.getCITY_CODE(), failInfo.getDEVICE_ID(), failInfo.getFAILURE_TYPE());
		
		/*String PK = failInfo.getCITY_CODE() +"_"+failInfo.getDEVICE_ID()+"_"+failInfo.getFAILURE_TIME()+"_"+failInfo.getFAILURE_TYPE();
		failInfo.setPK(PK);
		failureInfoPhoenixService.insert(failInfo);
		Date end1 = new Date();
		System.out.println("------------- 写入HBase时间： " + ((end1.getTime() - start.getTime())) + "s -------------");
		failInfo.setDEVICE_ID("0509045335x");
		failureInfoPhoenixService.insert(failInfo);
		Date end2 = new Date();*/
		//System.out.println("------------- 写入HBase时间： " + ((end2.getTime() - end1.getTime())) + "s -------------");
		
		
		/*for(int i = 0; i < 1000; i++){
			Date start = new Date();
			FailureInfoPhoenixService failureInfoPhoenixService = (FailureInfoPhoenixService)ContextAccessor.getBean("failureInfoPhoenixService");
			FailureInfoPhoenix failInfo = new FailureInfoPhoenix();
			//BeanUtils.copyProperties(fi, failInfo);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			failInfo.setCITY_CODE("4401" + i);
			failInfo.setCITY_NAME("广州");
			failInfo.setCREATE_TIME(sdf.format(new Date()));
			failInfo.setDEVICE_ID("0509045335s" + i);
			failInfo.setDEVICE_NAME("区庄5335");
			failInfo.setDEVICE_TYPE("02");
			failInfo.setDEVICE_TYPE_NAME("闸机");
			failInfo.setFAILURE_TIME("2017-11-03 11:46:09");
			failInfo.setLINE_CODE("05");
			failInfo.setLINE_NAME("5号线");
			failInfo.setSTATION_CODE("0509");
			failInfo.setSTATION_NAME("区庄");
			failInfo.setSTATUS_VALUE("1");
			failInfo.setFAILURE_TYPE("1");
			failInfo.setRECOVER_TIME(sdf.format(new Date()));
			String PK = failureInfoPhoenixService.selectPK(failInfo.getCITY_CODE(), failInfo.getDEVICE_ID(), failInfo.getFAILURE_TYPE());
			if(PK == null){
				PK = failInfo.getCITY_CODE() +"_"+failInfo.getDEVICE_ID()+"_"+failInfo.getFAILURE_TIME()+"_"+failInfo.getFAILURE_TYPE();
			}
			failInfo.setPK(PK);
			failureInfoPhoenixService.insert(failInfo);
			Date end = new Date();
			System.out.println("------------- 写入HBase时间： " + ((end.getTime() - start.getTime())) + "s -------------");
		}*/
		
		
	}
	

}

