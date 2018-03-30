package com.shankephone.data.storage.spring;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class ParseXmlTest {

	@Test
	public void parseXmlTest() {
		
		try {
			InputStream resourceAsStream = ParseXmlTest.class.getClassLoader().getResourceAsStream("tablesMapping.xml");
			InputSource inputSource = new InputSource(resourceAsStream);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = factory.newDocumentBuilder();
			Document document = db.parse(inputSource);
			Element root = document.getDocumentElement();
			NodeList nodeList = root.getChildNodes();

			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				
				if (node.getNodeType() == Node.ELEMENT_NODE && "table".equals(node.getNodeName())) {
					// 取得节点的属性值
					String id = node.getAttributes().getNamedItem("mysql").getNodeValue();
					System.out.println("mysql:" + id);
				}

			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		

	}

}
