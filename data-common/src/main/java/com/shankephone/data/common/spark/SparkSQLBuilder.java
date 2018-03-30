package com.shankephone.data.common.spark;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.util.ClassUtils;
import com.shankephone.data.common.util.FreemarkerUtils;
import com.shankephone.data.common.util.PropertyAccessor;
/**
 * Spark SQL相关构造器
 * @author DuXiaohua
 * @version 2017年9月22日 上午10:47:07
 */
public class SparkSQLBuilder extends SparkBuilder {
	
	private final static Logger logger = LoggerFactory.getLogger(SparkSQLBuilder.class);

	private SparkSession session;
	
	public SparkSQLBuilder(Class clazz) {
		super(clazz);
	}

	public SparkSQLBuilder(String appName) {
		super(appName);
	}

	protected void mergeConfig() {
		super.mergeConfig();
		Properties sparkStreamingConfig = PropertyAccessor.getProperties("default.sql.spark");
		Properties appConfig = PropertyAccessor.getProperties(appName + ".sql.spark");
		config.putAll(sparkStreamingConfig);
		config.putAll(appConfig);
	}
	
	/**
	 * 获取SparkSession，如果没有则创建。
	 * master、appName均取自配置文件，appName会自动添加后缀.SQL
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:48:02
	 * @return
	 */
	public SparkSession getSession() {
		if (session == null) {
			session = SparkSession.builder()
					              .master(config.getProperty("master"))
					              .appName(realAppName + ".SQL")
					              .enableHiveSupport()
					              .getOrCreate();
		}
		return session;
	}
	
	/**
	 * 创建HBase表对应的Dataset，自动使用tableName对应的Schema文件
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:51:36
	 * @param tableName
	 * @return
	 */
	public Dataset<Row> createHBaseDataset(String tableName) {
		try {
			String tableSchemaJson = IOUtils.toString(ClassUtils.getResourceAsStream("schemas/" + tableName + ".json"), "UTF-8");
			return getSession().read()
							   .option(HBaseTableCatalog.tableCatalog(), tableSchemaJson)
							   .format("org.apache.spark.sql.execution.datasources.hbase")
							   .load();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 创建HBase表对应的Spark View，首先会创建Dataset，参见{@link #createHBaseDataset(String)}，
	 * View的名称为tableName
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:54:07
	 * @param tableName
	 * @return
	 */
	public Dataset<Row> createHBaseView(String tableName) {
		Dataset<Row> dataset = createHBaseDataset(tableName);
		dataset.createOrReplaceTempView(tableName);
		return dataset;
	}
	
	/**
	 * 执行指定模版的Spark SQL，此方法不会创建Dataset和View
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:56:40
	 * @param sqlName
	 * @param sqlParams
	 * @return
	 */
	public Dataset<Row> executeSQL(String sqlName, Map<String,Object> sqlParams) {
		String sql = FreemarkerUtils.create().getTemplateText(sqlName, sqlParams);
		logger.info(sql);
		return getSession().sql(sql);
	}
	
	/**
	 * 创建Spark View，并执行指定模版的SQL
	 * @author DuXiaohua
	 * @date 2017年9月22日 上午10:57:32
	 * @param tableName
	 * @param sqlName
	 * @param sqlParams
	 * @return
	 */
	public Dataset<Row> executeHBaseSQL(String tableName, String sqlName, Map<String,Object> sqlParams) {
		createHBaseView(tableName);
		return executeSQL(sqlName, sqlParams);
	}
	
}
