package com.shankephone.data.collecting.config;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.FIELD)
public class Table implements Serializable {

	private static final long serialVersionUID = 6937687706366746911L;
	@XmlAttribute
	private String name;
	@XmlAttribute
	private String schema;
	@XmlAttribute
	private String hashColumn;
	@XmlAttribute
	private String kafkaTopic;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getHashColumn() {
		return hashColumn;
	}

	public void setHashColumn(String hashColumn) {
		this.hashColumn = hashColumn;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	@Override
	public String toString() {
		return String.format("Table [name=%s, schema=%s, hashColumn=%s, kafkaTopic=%s]", name, schema, hashColumn,
				kafkaTopic);
	}

}
