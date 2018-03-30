package com.shankephone.data.computing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.hbase.HBaseClient;
import com.shankephone.data.common.spark.SparkSQLBuilder;

public class HbaseDelete implements Executable{
	
	private final static Logger logger = LoggerFactory.getLogger(HbaseDelete.class);
	
	@Override
	public void execute(Map<String, Object> args) {
		String sql = (String) args.get("sql");
		String tableName = (String) args.get("tableName");
		logger.info("SQL: " + sql + "\ntableName = " + tableName);
		if (sql != null && tableName != null) {
			HbaseDelete hDelete = new HbaseDelete();
			SparkSQLBuilder builder = new SparkSQLBuilder(hDelete.getClass());
			hDelete.hbaseDelete(builder, sql, tableName);
			builder.getSession().close();
			logger.info("delete " + tableName + " success!");
		} else {
			logger.info("parameters missing!");
		}
		
	}

	/**
	 * 
	 * @author senze
	 * @date 2018年3月12日 下午12:37:56
	 * @param sql  //select rowkey from tablename where ...
	 * @param tableName
	 */
	public void hbaseDelete(SparkSQLBuilder builder, String sql, String tableName) {
		Dataset<Row> rows = builder.getSession().sql(sql);

		rows.foreachPartition(row -> {
			Connection con = HBaseClient.getInstance().getConnection();
			Table table = con.getTable(TableName.valueOf(tableName));

			int count = 100;
			List<Delete> deletes = new ArrayList<Delete>();
			while (row.hasNext()) {
				Row r = row.next();
				String rowKey = (String)r.get(0);
				deletes.add(new Delete(Bytes.toBytes(rowKey)));
				if ((--count) < 1) {
					table.delete(deletes);
					count = 100;
					deletes.clear();
				}
			}
			if (deletes != null && deletes.size() > 0) {
				table.delete(deletes);
			}
			table.close();
		});		
	}
	
}
