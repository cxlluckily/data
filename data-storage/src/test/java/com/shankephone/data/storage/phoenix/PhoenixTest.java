package com.shankephone.data.storage.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class PhoenixTest {
	private Connection con = null;
	
	@Before
	public void getConnect(){
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			final ExecutorService exec = Executors.newFixedThreadPool(1);
			Callable<Connection> call = new Callable<Connection>() {
				public Connection call() throws Exception {
					Properties pro = new Properties();
					pro.setProperty("hbase.rpc.timeout", "600000");
					pro.setProperty("hbase.client.scanner.timeout.period", "600000");
					pro.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
					return DriverManager.getConnection("jdbc:phoenix:data0.shankephone.com,data1.shankephone.com,data2.shankephone.com,data3.shankephone.com,data4.shankephone.com:2181:/hbase-unsecure",pro);
				}
			};
			Future<Connection> future = exec.submit(call);
			// 如果在5s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
			con = future.get(1000 * 15, TimeUnit.MILLISECONDS);
			exec.shutdownNow();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void executeGlobalIndexSql(){
		try {
			 // 耗时监控：记录一个开始时间
            long startTime = System.currentTimeMillis();
            if (con == null) {
                throw new Exception("Phoenix DB连接超时！");
            }
            Statement stmt = con.createStatement();
            PhoenixResultSet set = (PhoenixResultSet) stmt.executeQuery("select * from SHANKEPHONE.TEST_TEMP2");
            ResultSetMetaData meta = set.getMetaData();
            ArrayList<String> cols = new ArrayList<String>();
            JSONArray jsonArr = new JSONArray();
            while (set.next()) {
                if (cols.size() == 0) {
                    for (int i = 1, count = meta.getColumnCount(); i <= count; i++) {
                        cols.add(meta.getColumnName(i));
                    }
                }

                JSONObject json = new JSONObject();
                for (int i = 0, len = cols.size(); i < len; i++) {
                    json.put(cols.get(i), set.getString(cols.get(i)));
                }
                jsonArr.add(json);
            }
            // 耗时监控：记录一个结束时间
            long endTime = System.currentTimeMillis();

            // 结果封装
            JSONObject data = new JSONObject();
            data.put("data", jsonArr);
            data.put("cost", (endTime - startTime) + " ms");
            System.out.println(data.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void executeLocalIndexSql(){
		try {
			 // 耗时监控：记录一个开始时间
            long startTime = System.currentTimeMillis();
            if (con == null) {
                throw new Exception("Phoenix DB连接超时！");
            }
            Statement stmt = con.createStatement();
            PhoenixResultSet set = (PhoenixResultSet) stmt.executeQuery("select * from \"owner_order\" where \"d\".\"ORDER_NO\"='01201706091438189122'");
            ResultSetMetaData meta = set.getMetaData();
            ArrayList<String> cols = new ArrayList<String>();
            JSONArray jsonArr = new JSONArray();
            while (set.next()) {
                if (cols.size() == 0) {
                    for (int i = 1, count = meta.getColumnCount(); i <= count; i++) {
                        cols.add(meta.getColumnName(i));
                    }
                }

                JSONObject json = new JSONObject();
                for (int i = 0, len = cols.size(); i < len; i++) {
                    json.put(cols.get(i), set.getString(cols.get(i)));
                }
                jsonArr.add(json);
            }
            // 耗时监控：记录一个结束时间
            long endTime = System.currentTimeMillis();

            // 结果封装
            JSONObject data = new JSONObject();
            data.put("data", jsonArr);
            data.put("cost", (endTime - startTime) + " ms");
            System.out.println(data.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void createTable(){
		try {
			 // 耗时监控：记录一个开始时间
	        long startTime = System.currentTimeMillis();
	        Statement stmt = con.createStatement();
	        stmt.executeUpdate("create table if not exists SHANKEPHONE.TEST_TEMP(PK VARCHAR primary key) default_column_family='D',VERSIONS=1,BLOOMFILTER='ROW',MAX_FILESIZE=8589934592 split on ('1','2','3','4','5','6','7','8','9','a','b','c','d','e','f')");
	        con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@After
	public void close(){
		try {
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
