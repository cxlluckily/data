package com.shankephone.data.common.spark.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
/**
 * Spark Streaming配置类，对应streamingConfig.xml
 * @author duxiaohua
 * @version 2018年2月7日 下午2:53:32
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="config")
public class StreamingConfig implements Serializable {

	private static final long serialVersionUID = 8899389527104618903L;
	
	@XmlElement(name="app")
	private List<StreamingApp> apps = new ArrayList<>();

	public List<StreamingApp> getApps() {
		return apps;
	}

	public void setApps(List<StreamingApp> apps) {
		this.apps = apps;
	}

	@Override
	public String toString() {
		return "StreamingConfig [apps=" + apps + "]";
	}
	
	public StreamingApp getApp(String appId) {
		for (StreamingApp app : apps) {
			if (app.getId().equals(appId)) {
				return app;
			}
		}
		return null;
	}
	
}
