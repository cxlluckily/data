package com.shankephone.data.common.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.util.PropertyAccessor;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.DefaultObjectWrapperBuilder;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

/**
 * freemarker模板工具类
 * @author fengql
 * @version 2017年9月19日 下午3:17:18
 */
public class FreemarkerUtils {
	
	private final static Logger logger = LoggerFactory.getLogger(FreemarkerUtils.class);
	private static FreemarkerUtils instance = new FreemarkerUtils();
	private static String directory = "/tpls";
	private static String encoding = "UTF-8";
	private static Configuration config = new Configuration(Configuration.VERSION_2_3_26);
	private static DefaultObjectWrapper wrapper = new DefaultObjectWrapperBuilder(Configuration.VERSION_2_3_26).build();
	private static ClassTemplateLoader loader = new ClassTemplateLoader(FreemarkerUtils.class, directory);
	
	private FreemarkerUtils(){}
	
	static {
		try {
			directory = PropertyAccessor.getProperty("freemarker.template.directory");
			config.setClassForTemplateLoading(FreemarkerUtils.class, directory);
		} catch (Exception e) {
			logger.error("setting template loading of freemarker configuration is error! please check directory.");
		}
		encoding = PropertyAccessor.getProperty("freemarker.template.encoding");
		config.setDefaultEncoding(encoding);
		config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		config.setLogTemplateExceptions(false);
		config.setObjectWrapper(wrapper);
		config.setTemplateLoader(loader);
	}
	
	public static FreemarkerUtils create(){
		return instance;
	}
	
	/**
	 * 获取模板动态文本内容
	 * @author fengql
	 * @date 2017年9月19日 下午2:37:54
	 * @param name
	 * @param params
	 * @return
	 */
	public String getTemplateText(String name, Map<String,Object> params){
		if (!name.endsWith(".ftl")) {
			name = name + ".ftl";
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		OutputStreamWriter out = new OutputStreamWriter(bos);
		try {
			Template template = config.getTemplate(name);
			template.process(params, out);
			byte [] bytes = bos.toByteArray();
			String text = new String(bytes,"utf-8");
			return text;
		} catch (Exception e) {
			logger.error("processing freemarker template is error!");
			throw new RuntimeException(e);
		} 
	}
	
	/*public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Map<String, Object> root = new HashMap<String,Object>();
		root.put("lastTime", new Date().getTime() + "");
		root.put("day", sdf.format(new Date()));
		String tpl = FreemarkerUtils.create().getTemplateText("order_source.ftl",root);
		System.out.println(tpl);
	}*/

}
