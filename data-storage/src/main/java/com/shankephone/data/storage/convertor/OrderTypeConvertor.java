package com.shankephone.data.storage.convertor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

/**
 * 订单类型处理类 :
 * 1.单程票 order_type = '1' 包含：DCP或TVIP 
 * 2.圈存 4100 长沙充值 order_type = '2', 包含：CSDT NFC_SIM CSSH_A CSSH_I 包含：SIM或NFC 
 * 3.先付后享 order_type = '4', 包含： ZH_RAIL_A ZH_RAIL_I EMP_TICKET_I EMP_TICKET_A， 也包含：QRCD或UPON 
 * 4.先享后付order_type = '3', 在配置中处理，不在此处处理
 * 5.咖啡order_type = '5',在配置中处理，不在此处处理
 * 
 * @author fengql
 * @version 2018年3月28日 下午4:49:17
 */
public class OrderTypeConvertor implements Convertible {

	private static Set<String> singleTicketSet = new HashSet<String>();
	private static Set<String> topupSet = new HashSet<String>();
	private static Set<String> qrSet = new HashSet<String>();
	
	private static String ORDER_TYPE_SINGLE = "1";
	private static String ORDER_TYPE_TOPUP = "2";
	private static String ORDER_TYPE_QR = "3";

	static {
		singleTicketSet.add("DCP");
		singleTicketSet.add("TVIP");

		topupSet.add("CSDT");
		topupSet.add("NFC_SIM");
		topupSet.add("CSSH_A");
		topupSet.add("CSSH_I");
		topupSet.add("SIM");
		topupSet.add("NFC");

		qrSet.add("ZH_RAIL_A");
		qrSet.add("ZH_RAIL_I");
		qrSet.add("EMP_TICKET_I");
		qrSet.add("EMP_TICKET_A");
	}

	@Override
	public Map<String, String> getValue(String name, JSONObject json) {
		String productCode = json.getString("PRODUCT_CODE");
		Map<String,String> map = new HashMap<String,String>();
		for(String key : singleTicketSet){
			if(productCode.contains(key)){
				map.put(name, ORDER_TYPE_SINGLE);
				return map;
			}
		}
		for(String key : topupSet){
			if(productCode.contains(key)){
				map.put(name, ORDER_TYPE_TOPUP);
				return map;
			}
		}
		for(String key : qrSet){
			if(productCode.contains(key)){
				map.put(name, ORDER_TYPE_QR);
				return map;
			}
		}
		map.put(name, ORDER_TYPE_SINGLE);
		return map;
	}

}
