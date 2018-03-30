package com.shankephone.data.common.computing;

import java.util.Map;
/**
 * 结合com.shankephone.data.common.computing.Main一起使用。
 * 执行main方法启动的程序，无需编写main方法，可以实现此接口并结合com.shankephone.data.common.computing.Main，来更便捷的获取主方法传递的参数。
 * @author duxiaohua
 * @version 2018年2月7日 下午3:11:00
 */
public interface Executable {
	/**
	 * 具体的业务方法，com.shankephone.data.common.computing.Main执行时会调用此方法并将main方法的String[]参数转换为Map传递进来。
	 * @author duxiaohua
	 * @date 2018年2月7日 下午3:14:37
	 * @param args
	 */
	public void execute(Map<String, Object> args);
	
}
