/*
 * Copyright(C) 2014,  CSSWEB TECHNOLOGY CO.,LTD.
 * All rights reserved.
 *
 */

package com.shankephone.data.common.test;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 普通基于JUnit的单元测试可以继承此类
 * @author DuXiaohua
 * @version 2015年4月8日 下午1:56:18
 */
public abstract class TestCase extends Assert {
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
}
