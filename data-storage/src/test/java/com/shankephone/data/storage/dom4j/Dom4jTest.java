package com.shankephone.data.storage.dom4j;

import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.Before;
import org.junit.Test;

public class Dom4jTest {
	private Document document = null;
	
	@Before
	public void getDocument(){
	    try {
	    	SAXReader reader = new SAXReader();              
	    	document = reader.read(Dom4jTest.class.getClassLoader().getResourceAsStream("tablesMapping.xml"));
		} catch (Exception e) {
			e.printStackTrace();
		}  
	}
	
	
	@Test
	public void testElement(){
		 try {
			 Element root = document.getRootElement();
			 for(Iterator it=root.elementIterator();it.hasNext();){        
				 Element element = (Element) it.next();  
				 System.out.println(element.attribute("mysql").getValue());
			 }
		 } catch (Exception e) {
				e.printStackTrace();
		}  
		
	}
	
	@Test
	public void testChildElement(){
		try {
			 Element root = document.getRootElement();
			 for(Iterator it=root.elementIterator();it.hasNext();){        
				 Element element = (Element) it.next();  
				 for(Iterator itt=element.elementIterator();itt.hasNext();){   
					 Element childs = (Element) itt.next();  
					 System.out.println(childs.getName());
				 }
			 }
		 } catch (Exception e) {
				e.printStackTrace();
		}  
	}

}
