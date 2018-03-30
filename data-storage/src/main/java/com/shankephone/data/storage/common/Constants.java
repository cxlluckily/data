package com.shankephone.data.storage.common;

public class Constants {
	/**
	 * 处理类的默认路径
	 */
	public static final String HANDLER_CLASS_PACKAGE = "com.shankephone.data.storage.convertor";
	/**
	 * 获取kafka中 JSON数据的key名称
	 */
	public static final String RECIEVE_JSON_KEY_NAME = "keys";
	
	
	/**
	 * 条件类型：must为and条件，should为or条件，not为非条件
	 * @author fengql
	 * @version 2018年2月1日 下午4:16:07
	 */
	public static enum RequireType {		
				
		MUST,SHOULD,NOT;
		
		public String getValue(){
			return name().toLowerCase();
		}
		
		
	}
	
	public static void main(String[] args) {
		System.out.println(RequireType.MUST.getValue()); 
		System.out.println(RequireType.MUST.getValue()); 
	}
	
	

}


