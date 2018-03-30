package com.shankephone.data.common.util;

import java.io.OutputStream;
import java.net.URL;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class XmlUtils {
	
	public static <T> T xml2Java(String resource, Class<T> clazz) {
		try {
			URL xmlUrl = ClassUtils.getResource(resource);
			JAXBContext  context = JAXBContext.newInstance(clazz);
			Unmarshaller u = context.createUnmarshaller();
			return (T) u.unmarshal(xmlUrl);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static <T> void java2XmlString(T t, OutputStream output) throws JAXBException {
		JAXBContext  context = JAXBContext.newInstance(t.getClass());
		Marshaller m = context.createMarshaller();
		m.marshal(t, output);
	}
	
	
}
