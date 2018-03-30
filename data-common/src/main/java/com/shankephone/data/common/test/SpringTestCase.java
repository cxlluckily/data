/*
 * Copyright(C) 2014,  CSSWEB TECHNOLOGY CO.,LTD.
 * All rights reserved.
 *
 */

package com.shankephone.data.common.test;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
/**
 * 需要spring上下文的单元测试可以继承此类
 * @author DuXiaohua
 * @version 2015年4月8日 下午1:57:07
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext.xml"})
public abstract class SpringTestCase extends TestCase {
}
