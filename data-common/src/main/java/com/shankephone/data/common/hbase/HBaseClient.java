package com.shankephone.data.common.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;
/**
 * HBase客户端，提供对HBase的基本操作
 * @author DuXiaohua
 * @version 2017年11月1日 下午2:30:01
 */
public class HBaseClient {
	
	public static final byte[] FAMILY_DEFAULT = Bytes.toBytes("D");
	
	private static volatile HBaseClient client;
	
	private Connection connection;
	
	private Admin admin = null;
	
	private HBaseClient() {
		try {
			connection = ConnectionFactory.createConnection();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static HBaseClient getInstance() {
		if (client == null) {
			newInstance();
		}
		return client;
	}
	
	private static synchronized void newInstance() {
		if (client == null) {
			client = new HBaseClient();
		}
	}
	/**
	 * 获取指定表默认列族“D”的所有数据
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:30:36
	 * @param tableName
	 * @return
	 */
	public List<Map<String, String>> getAll(String tableName) {
		try (Table table = connection.getTable(TableName.valueOf(tableName));
			 ResultScanner scanner = table.getScanner(new Scan());) {
			List<Map<String, String>> resultList = new ArrayList<>();
			for (Result result : scanner) {
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> rowMap = result.getNoVersionMap();
				NavigableMap<byte[], byte[]> rowFamilyMap = rowMap.get(FAMILY_DEFAULT);
				Map<String, String> rowFamilyStrMap= new HashMap<>();
				for (Entry<byte[], byte[]> entry : rowFamilyMap.entrySet()) {
					rowFamilyStrMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
				}
				resultList.add(rowFamilyStrMap);
			}
			return resultList;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 获取指定表默认列族“D”的所有数据，转换为指定类型的对象
	 * @author DuXiaohua
	 * @date 2017年11月1日 下午2:30:36
	 * @param tableName
	 * @return
	 */
	public <T> List<T> getAll(String tableName, Class<T> type) {
		List<Map<String, String>> mapList = getAll(tableName);
		List<T> resultList = new ArrayList<T>();
		for (Map<String, String> map : mapList) {
			try {
				T obj = type.newInstance();
				for (Entry<String, String> entry : map.entrySet()) {
					String name = underlineToCamelCase(entry.getKey());
					BeanUtils.copyProperty(obj, name, entry.getValue());
				}
				resultList.add(obj);
			}  catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return resultList;
	}
	
	private String underlineToCamelCase(String str) {
		String[] strArray = null;
		try {
			strArray = str.toLowerCase().split("_");
			for (int i = 1; i < strArray.length; i ++) {
				String first = strArray[i].substring(0, 1);
				strArray[i] = strArray[i].replaceFirst(first, first.toUpperCase());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return String.join("", strArray);
	}
	

	/**
	 * TODO 修改rowkey读取方式
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public JSONObject getSingleRowByKeyValue(String tableName,String rowKey){
		JSONObject resultJson = new JSONObject();
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			//获取row
			Result result = table.get(new Get(Bytes.toBytes(rowKey)));
			//循环里面的column
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()){
				Cell currentCell = cellScanner.current();
				byte[] qualifierArray = currentCell.getQualifierArray();
				byte[] valueArray = currentCell.getValueArray();
				resultJson.put(new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength()), new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength()));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("查询hbase中表为["+tableName+"]key为["+rowKey+"]的row， 异常!", e);
		}
		return resultJson;
	}
	
	/**
	 * @todo 获取表中多行数据
	 * @author sunweihong
	 * @param tableName
	 * @param beginKey 起始key,如果为null则默认为0
	 * @param endKey 结束key,如果为null则默认为table.length
	 * @return
	 * 2017年4月20日 下午1:49:59
	 */
	public JSONObject getMultipleRowsByKeyValue(String tableName,String beginKey,String endKey,long maxResultSize){
		JSONObject resultJson = new JSONObject();
		JSONObject row = null;
		ResultScanner scanner = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if(null != beginKey && !"".equals(beginKey)){
				scan.setStartRow(Bytes.toBytes(beginKey));
			}
			if(null != endKey && !"".equals(endKey)){
				scan.setStopRow(Bytes.toBytes(endKey+"\000"));
			}
			Filter filter = new PageFilter(maxResultSize); 
			scan.setCaching(500);
			scan.setFilter(filter);
			scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			while(iterator.hasNext()){
				//获取row
				Result result = iterator.next();
				//循环里面的column
				CellScanner cellScanner = result.cellScanner();
				while(cellScanner.advance()){
					Cell currentCell = cellScanner.current();
					byte[] familyArray = currentCell.getFamilyArray();
					byte[] qualifierArray = currentCell.getQualifierArray();
					byte[] valueArray = currentCell.getValueArray();
					byte[] rowArray = currentCell.getRowArray();
					String curretnRowKey = new String(rowArray, currentCell.getRowOffset(), currentCell.getRowLength());
					if(resultJson.containsKey(curretnRowKey)){
						row = resultJson.getJSONObject(curretnRowKey);
					}else{
						row = new JSONObject();
					}
					row.put(new String(familyArray,currentCell.getFamilyOffset(),currentCell.getFamilyLength()) +"@"+new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength()), new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength()));
					resultJson.put(curretnRowKey, row);
				}
			}
//			logger.info("【hbase】 查询hbase中表为["+tableName+"]所有数据: "+resultJson.toJSONString());
			scanner.close();
		} catch (Exception e) {
			e.printStackTrace();
			scanner.close();
			throw new RuntimeException("【hbase】 查询hbase中表为["+tableName+"]所有数据，异常!");
		}
		return resultJson;
	}
	
	
	
	/**
	 * @todo 通过过滤器获取表中数据
	 * @author sunweihong
	 * @param tableName 表名
	 * @param filter 过滤器
	 * @return
	 * 2017年4月25日 下午2:41:27
	 */
	public JSONObject getRowsByFilters(String tableName,Filter filter){
		JSONObject resultJson = new JSONObject();
		JSONObject row = null;
		ResultScanner scanner = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setMaxVersions();
			scan.setFilter(filter);
			scan.setMaxResultSize(10);
			scanner = table.getScanner(scan);
			for(Result result : scanner){
				CellScanner cellScanner = result.cellScanner();
				while(cellScanner.advance()){
					Cell currentCell = cellScanner.current();
					byte[] familyArray = currentCell.getFamilyArray();
					byte[] qualifierArray = currentCell.getQualifierArray();
					byte[] valueArray = currentCell.getValueArray();
					byte[] rowArray = currentCell.getRowArray();
					String curretnRowKey = new String(rowArray, currentCell.getRowOffset(), currentCell.getRowLength());
					if(resultJson.containsKey(curretnRowKey)){
						row = resultJson.getJSONObject(curretnRowKey);
					}else{
						row = new JSONObject();
					}
					row.put(new String(familyArray,currentCell.getFamilyOffset(),currentCell.getFamilyLength()) +"@"+new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength()), new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength()));
					resultJson.put(curretnRowKey, row);
				}
			}
			scanner.close();
		} catch (Exception e) {
			e.printStackTrace();
			scanner.close();
			throw new RuntimeException("【hbase】 查询hbase中表为["+tableName+"]所有数据，异常!");
		}
		return resultJson;
	}
	
	
	public boolean close(){
		try {
			admin.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}


	public Connection getConnection() {
		return connection;
	}

	public Admin getAdmin() {
		return admin;
	}
	
}
