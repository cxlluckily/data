package com.shankephone.data.monitoring.computing.device.model;

import java.util.Date;

/**
 * 故障实时明细
 * @author fengqll
 * @version 2017年10月11日 上午11:19:06
 */
public class FailureDetailsHistory extends FailureDetails {

	private static final long serialVersionUID = 4772571530479731545L;
	//故障发生时间
	private Date recover_time;
	
	public Date getRecover_time() {
		return recover_time;
	}
	public void setRecover_time(Date recover_time) {
		this.recover_time = recover_time;
	}
}
