package com.shankephone.data.storage.convertor;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

/**
 * 银联闪付对应支付信息处理
 * @author fengql
 * @version 2018年2月2日 上午11:20:05
 */
public class PosPayConvertor implements Convertible {
	
	@Override
	public Map<String, String> getValue(String name, JSONObject json) {
		String createDate = json.getString("REG_DATE");
		String tradeDate = json.getString("TXN_DATE");
		String tradeTime = json.getString("TXN_TIME");
		String value = null;
		String year = createDate.substring(0,4);
		if(tradeDate != null && tradeDate.length() < 8){
			tradeDate = tradeDate.substring(0,2) + "-" + tradeDate.substring(2,4);
			tradeTime = tradeTime.substring(0,2) + ":" + tradeTime.substring(2,4)
					+ ":" + tradeTime.substring(4,6);
			value = year + "-" + tradeDate + " " + tradeTime;
		} else {
			value = tradeDate + " " + tradeTime;
		}
		Map<String,String> map = new HashMap<String,String>();
		map.put(name, value);
		return map;
	}
	
	public static void main(String[] args) {
		String str = "{\"logfileOffset\":498394430,\"columns\":{\"CARD_NO\":\"6222621010020490316\",\"TRACE\":\"008331\",\"TXN_TIME\":\"101750\",\"AUTH_CODE\":\"345694\",\"RESP_CODE\":\"00\",\"MER_BILL_NO\":\"\",\"SETTLEMENT_DATE\":\"\",\"RESP_CHIN\":\"交易成功\",\"MERID\":\"898053241120002\",\"REG_DATE\":\"2018-02-02 10:17:55\",\"ORDER_NO\":\"01201802021017483900\",\"TRANS_TYPE\":\"00\",\"BATCH_NO\":\"\",\"REFDATA\":\"101750596070\",\"TRANS_STATUS\":\"\",\"AMOUNT\":\"000000000400\",\"TXN_DATE\":\"0202\",\"BILL_DATE\":\"\",\"DISCOUNT_AMOUNT\":\"000000000000\",\"ID\":\"POSFD892989C024446FA0B349DD9FF58F05\",\"QRCODE_URL\":\"\",\"BANK_CODE\":\"0301\",\"TERID\":\"00029311\",\"LRC\":\"134\"},\"keys\":{\"ID\":\"POSFD892989C024446FA0B349DD9FF58F05\"},\"logfile\":\"ON.000202\",\"eventTypeName\":\"insert\",\"schemaName\":\"STTRADE2660\",\"serverId\":15,\"executeTime\":1517537875000,\"tableName\":\"OWNER_ORDER_POS_PAY_INFO\"}";
		JSONObject json = JSONObject.parseObject(str);
		PosPayConvertor con = new PosPayConvertor();
		System.out.println(con.getValue("schemaName", json));
	}
	

}
