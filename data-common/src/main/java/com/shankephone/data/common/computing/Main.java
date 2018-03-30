package com.shankephone.data.common.computing;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 通用的主方法，提供简单的参数传递方法。主要用来启动Spark应用程序。
 * 应用程序需要实现Executable接口。
 * 此方法支持的参数：
 * -class 实现Executable接口的具体业务实现类
 * -args key=value 键值对参数，可以有多个，如：-args a=1 -args b=2
 * 此方法将-args参数转换为Map，并调用Executable接口的execute方法。
 * @author duxiaohua
 * @version 2018年2月7日 下午3:00:08
 */
public class Main {
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static String ARG_NAME_MAIN_CLASS = "class";
	private final static String ARG_NAME_OTHER = "args";
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option mainClass = Option.builder(ARG_NAME_MAIN_CLASS)
												.argName("className")
												.required()
												.hasArg()
												.desc("main class name")
												.build();
		Option argument = Option.builder(ARG_NAME_OTHER)
											   .argName("key=value")
											   .hasArg()
											   .numberOfArgs(2)
											   .valueSeparator()
											   .desc("argument's name and value")
											   .build();
		options.addOption(mainClass);
		options.addOption(argument);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		String mainClassName = cmd.getOptionValue(ARG_NAME_MAIN_CLASS);
		Map<String, Object> arguments = (Map)cmd.getOptionProperties(ARG_NAME_OTHER);
		
		logger.info("Execute Class : [" + mainClassName + "] with args : [" + arguments + "]");
		Class<Executable> clazz = (Class<Executable>) Class.forName(mainClassName);
		Executable executable = clazz.newInstance();
		executable.execute(arguments);
	}
}
