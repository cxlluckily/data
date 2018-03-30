package com.shankephone.data.monitoring.computing.device.model;

import java.util.Date;

import com.shankephone.data.monitoring.computing.common.model.BaseModel;

/**
 * 故障实时记录
 * @author fengql
 * @version 2017年10月11日 上午11:05:54
 */
public class FailureInfo extends BaseModel {

	private static final long serialVersionUID = 4772571530479731545L;
	//主键标识
	protected Long id;
	//城市代码
	protected String city_code;
	//城市名称
	protected String city_name;
	//线路代码
	protected String line_code;
	//线路名称
	protected String line_name;
	//站点代码
	protected String station_code;
	//站点名称
	protected String station_name;
	//设备ID
	protected String device_id;
	//设备名称
	protected String device_name;
	//设备类型：01-购票机,2-闸机
	protected String device_type;
	//设备类型名称
	protected String device_type_name;
	//故障类型：0-故障，1-离线
	protected String failure_type;
	//设备状态
	protected String status_value;
	//设备所在出口
	protected String area_code;
	//故障发生时间
	protected Date failure_time;
	//记录创建时间
	protected Date create_time;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getCity_code() {
		return city_code;
	}

	public void setCity_code(String city_code) {
		this.city_code = city_code;
	}

	public String getCity_name() {
		return city_name;
	}

	public void setCity_name(String city_name) {
		this.city_name = city_name;
	}

	public String getLine_code() {
		return line_code;
	}

	public void setLine_code(String line_code) {
		this.line_code = line_code;
	}

	public String getLine_name() {
		return line_name;
	}

	public void setLine_name(String line_name) {
		this.line_name = line_name;
	}

	public String getStation_code() {
		return station_code;
	}

	public void setStation_code(String station_code) {
		this.station_code = station_code;
	}

	public String getStation_name() {
		return station_name;
	}

	public void setStation_name(String station_name) {
		this.station_name = station_name;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public String getDevice_name() {
		return device_name;
	}

	public void setDevice_name(String device_name) {
		this.device_name = device_name;
	}

	public String getDevice_type() {
		return device_type;
	}

	public void setDevice_type(String device_type) {
		this.device_type = device_type;
	}

	public String getDevice_type_name() {
		return device_type_name;
	}

	public void setDevice_type_name(String device_type_name) {
		this.device_type_name = device_type_name;
	}

	public String getFailure_type() {
		return failure_type;
	}

	public void setFailure_type(String failure_type) {
		this.failure_type = failure_type;
	}

	public String getStatus_value() {
		return status_value;
	}

	public void setStatus_value(String status_value) {
		this.status_value = status_value;
	}

	public String getArea_code() {
		return area_code;
	}

	public void setArea_code(String area_code) {
		this.area_code = area_code;
	}

	public Date getFailure_time() {
		return failure_time;
	}

	public void setFailure_time(Date failure_time) {
		this.failure_time = failure_time;
	}

	public Date getCreate_time() {
		return create_time;
	}

	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}

	
	
}
