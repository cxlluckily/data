package com.shankephone.data.storage.common;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.test.SpringTestCase;
import com.shankephone.data.common.util.PropertyAccessor;

public class CommonTest extends SpringTestCase {
	private static final LinkedHashMap<String, String> obj = null;

	/** 
	 * @todo 加载sttradeXXX到City_Code的映射关系
	 * @author sunweihong
	 * @date 2017年7月6日 上午11:23:55
	 */
	@Test
	public void loadSttradeXXXToCityCodeMap(){
		try {
			String tmp = PropertyAccessor.getProperty("constants.sttradexxx.to.citycode.map");
			String[] arr = tmp.split(",");
			for(String one : arr){
				String[] maps = one.split(":");
				System.out.println(maps[0] +" - "+ maps[1]);
			}
			
			String tmp2 = PropertyAccessor.getProperty("constants.partnerno.to.citycode.map");
			String[] arr2 = tmp2.split(",");
			for(String one2 : arr2){
				String[] maps2 = one2.split(":");
				System.out.println(maps2[0] +" - "+ maps2[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void getFromMap(){
		ConcurrentHashMap<String,List<String>> nodeMap = new ConcurrentHashMap<String,List<String>>();
		List<String> lst = nodeMap.get("test");
		if(lst==null){
			
			System.out.println(111);
		}
	}
	
	@Test
	public void testFor(){
		for(int i=0;i<=10;i++){
			System.out.println(i);
			if(i==5){
				return;
			}
		}
	}
	
	@Test
	public void testObject(){
		
		LinkedHashMap<String,Object> hcolumns = new LinkedHashMap<String,Object>();
		LinkedHashMap<String,String> hcolumnsMaps = new LinkedHashMap<String,String>();
		hcolumnsMaps.put("123", "value");
		hcolumns.put("one", "dada");
		hcolumns.put("on2e", hcolumnsMaps);
		Set<String> keySet = hcolumns.keySet();
		for(String set : keySet){
			Object obj = hcolumns.get(set);
			if(obj instanceof String){
				String str = (String)obj;
				System.out.println(str); 
			}else if(obj instanceof LinkedHashMap){
				LinkedHashMap<String,String> tmp = (LinkedHashMap<String, String>) obj;
				Set<String> keySet2 = tmp.keySet();
				for(String set2 : keySet2){
					System.out.println(tmp.get(set2));
				}
			}
		}
		
	}
	
	@Test
	public void testJSON(){
		JSONObject singleRowByKeyValue = new JSONObject();
		singleRowByKeyValue.put("dd", "aa");  
		System.out.println(singleRowByKeyValue.isEmpty());
	}
	
	@Test
	public void testJSONMap1(){
		JSONObject json = new JSONObject();
		JSONObject t = new JSONObject();
		
		t.put("a", "aa");
		t.put("b", "bb");
		json.put("cs",t);
		Map<String,String> m = new HashMap<String,String>();
		m.put("a", "a1");
		m.put("c", "c1");
		
		t.putAll(m);
		json.put("cs",t);
		System.out.println(json.toJSONString());
	}
	
	@Test
	public void testJSONMap(){
		/*JSONObject json = new JSONObject();
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("name", "xl");
		map.put("age", 22);
		map.put("man", false);
		json.put("person", map);
		System.out.println(json.toJSONString());*/
		
		String a = "12345";
		String s = a.substring(0,4);
		System.out.println(s);
	}
	
	@Test
	public void testFastDateFormat() throws ParseException{
		FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
		String str = "2017-04-21 10:39:18.0";
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(fastDateFormat.parse(str));
		calendar.add(Calendar.DATE, -1);
		
		System.out.println(fastDateFormat.format(calendar));
	}
	
	@Test
	public void testMap() {
		Map<String, String> m = new HashMap<String, String>();
		m.put("a", "1");
		m.put("b", "2");
		m.put("c", "3");
		
		Map<String, String> n = m;
		n.remove("a");
		
		System.out.println(m);
		System.out.println(n);
		
	}
	
}
