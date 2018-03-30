package com.shankephone.data.common.spark.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
/**
 * Spark Streaming应用，由多个串行的processor组成。
 * @author duxiaohua
 * @version 2018年2月7日 下午2:55:59
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class StreamingApp implements Serializable {
	
	private static final long serialVersionUID = 7461497922231585888L;
	/**
	 * 应用的唯一标识，至少在同一个配置文件中唯一
	 */
	@XmlAttribute
	private String id;
	@XmlElement(name="processor")
	private List<String> processors = new ArrayList<>();
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getProcessors() {
		return processors;
	}

	public void setProcessors(List<String> processors) {
		this.processors = processors;
	}

	@Override
	public String toString() {
		return "StreamingApp [id=" + id + ", processors=" + processors + "]";
	}
	
}
