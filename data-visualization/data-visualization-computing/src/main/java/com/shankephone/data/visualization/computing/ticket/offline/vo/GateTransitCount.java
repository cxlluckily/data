package com.shankephone.data.visualization.computing.ticket.offline.vo;

import java.io.Serializable;

public class GateTransitCount implements Serializable {

	private static final long serialVersionUID = 4036836928174095200L;

	private String tranDate;
	private String productCategory;
	private Integer total;

	public String getTranDate() {
		return tranDate;
	}

	public void setTranDate(String tranDate) {
		this.tranDate = tranDate;
	}

	public String getProductCategory() {
		return productCategory;
	}

	public void setProductCategory(String productCategory) {
		this.productCategory = productCategory;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}
	
}