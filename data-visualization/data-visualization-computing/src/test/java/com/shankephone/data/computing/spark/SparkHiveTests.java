package com.shankephone.data.computing.spark;

import org.apache.spark.sql.SparkSession;

public class SparkHiveTests {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .master("local[*]")
				  .appName("SparkHiveTests")
				  .config("hive.metastore.uris", "thrift://data3.test:9083")
				  .enableHiveSupport()
				  .getOrCreate();
		spark.sql("select * from shankephone.order_info limit 10").show();
		spark.close();
	}
}
