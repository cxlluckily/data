package com.shankephone.data.computing;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestUtils {
	
	@Test
	public void testStr(){
		String a = "2017-09-21 12:00:02";
		System.out.println(a.substring(0,16));
	}
	
	@Test
	public void compareString(){
		String a = "2018-02-06 17:19:14";
		String b = "2018-02-06 17:19:12";
		System.out.println(a.compareTo(b) > 0);
	}
	
	@Test
	public void testMap(){
		Map<String,Object> m = new HashMap<String,Object>();
		m.put("a","aa");
		m.put(null,"aa");
		m.put("b", 2);
		System.out.println(m.toString());
	}
	
}
