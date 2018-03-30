package com.shankephone.data.storage.common;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.util.XmlUtils;
import com.shankephone.data.storage.common.Constants.RequireType;
import com.shankephone.data.storage.config.StorageConfig;
import com.shankephone.data.storage.convertor.Convertible;
import com.shankephone.data.storage.exception.ConfigureException;
import com.shankephone.data.storage.exception.SyncException;
import com.shankephone.data.storage.pojo.Column;
import com.shankephone.data.storage.pojo.ConfigTable;
import com.shankephone.data.storage.pojo.Criteria;
import com.shankephone.data.storage.pojo.HashKeys;
import com.shankephone.data.storage.pojo.Identifiable;
import com.shankephone.data.storage.pojo.RowKeys;
import com.shankephone.data.storage.pojo.TagElement;

/**
 * 配置处理工具类
 * @author fengql
 * @version 2018年1月23日 下午7:48:02
 */
public class ConfigureUtil {
	
	/**
	 * 通过配置文件获取JAVA对象
	 * @param fileName
	 */
	public static StorageConfig loadConfigure(String fileName){
		//table元素的Map
		Map<String, List<ConfigTable>> tableMap = new ConcurrentHashMap<String, List<ConfigTable>>();
		//所有聚合的表，聚合表的table元素中有columnPrefix属性
		List<String> aggregations = new ArrayList<String>();
		//历史表映射
		Map<String, ConfigTable> historyMap = new ConcurrentHashMap<String, ConfigTable>();
		//聚合表的映射
		Map<String, List<ConfigTable>> aggrTables = new ConcurrentHashMap<String, List<ConfigTable>>();
		//单表映射
		Map<String, ConfigTable> singleTableMap = new HashMap<String,ConfigTable>();
		try {
			StorageConfig storageConfig = XmlUtils.xml2Java(fileName, StorageConfig.class);
			List<ConfigTable> tables = storageConfig.getTables();
			
			//存放表名与原表映射 
			for(ConfigTable table : tables){  
				//mysql表
				String mysql = table.getMysql();
				//hbase表
				String hbase = table.getHbase();
				
				if(mysql==null || hbase==null || table.getColumnFamily()==null){
					throw new ConfigureException("【ERROR】解析配置文件出错，table标签中必须有mysql、hbase、columnFamily三个基础属性！");
				}
				//遍历column列表，在没有配置列的情况下将得不到列，需要在接收数据时组装获取
				List<Column> columns = table.getColumns();
				if(columns != null && columns.size() > 0){
					for(Column col : columns){
						//取列名做映射
						Map<String,String> nameMap = getColumnName(col);
						String colName = nameMap.get("mysql");
						if(colName == null || "".equals(colName)){
							colName = nameMap.get("hbase");
						}
						List<Column> colList = table.getColumnsMap().get(colName);
						if (colList == null) {
							colList = new ArrayList<>();
							table.getColumnsMap().put(colName, colList);
						}
						colList.add(col);
					}
				} 
				List<ConfigTable> lst = tableMap.get(mysql);
				if(null == lst){
					lst = new ArrayList<ConfigTable>(); 
				}
				//存储表映射
				lst.add(table);
				tableMap.put(mysql, lst);
				
				//分析聚合表,即创建hbase表名与表的映射
				List<ConfigTable> originalTables = aggrTables.get(table.getHbase());
				if(originalTables == null){
					originalTables = new ArrayList<ConfigTable>();
				}
				originalTables.add(table);
				aggrTables.put(table.getHbase(), originalTables);
			}
			
			//组装聚合表与单表
			Set<String> keySet = aggrTables.keySet();
			for(Iterator<String> it = keySet.iterator(); it.hasNext(); ){
				String key = it.next();
				List<ConfigTable> tableList = aggrTables.get(key);
				if(tableList.size() <= 1){
					//如果长度为1，则是单表，并组装单表
					ConfigTable table = tableList.get(0);
					singleTableMap.put(table.getMysql(), table);
				} else {
					//组装聚合表
					aggregations.add(key);
				}
			}
			
			//设置聚合表
			storageConfig.setAggregations(aggregations);
			//全部表映射
			storageConfig.setTableMap(tableMap);
			//设置单表映射
			storageConfig.setSingleTableMap(singleTableMap);
			//生成历史表映射
			historyMap = generateHistoryMap(singleTableMap);
			//设置历史表映射
			storageConfig.setHistoryMap(historyMap);
			
			return storageConfig;
		} catch (Exception e) {
			throw new ConfigureException(e, "storageConfig配置文件有误！请检查");
		}
		
	}

	/**
	 * 生成历史表
	 * @param singleTableMap
	 * @return
	 */
	private static Map<String, ConfigTable> generateHistoryMap(
			Map<String, ConfigTable> singleTableMap) {
		Map<String, ConfigTable> historyMap = new HashMap<String,ConfigTable>();
		//hbase映射
		Map<String,ConfigTable> hbaseMap = new HashMap<String,ConfigTable>();
		//设置历史表数据预处理
		for(String tableName : singleTableMap.keySet()){
			ConfigTable tb = singleTableMap.get(tableName);
			//配置的hbase历史表
			String history = tb.getHbaseHist();
			if(history != null && !"".equals(history)){
				historyMap.put(history, null);
			}
			hbaseMap.put(tb.getHbase(), tb);
		}
		//组装历史表
		for(String hbase : historyMap.keySet()){
			historyMap.put(hbase, hbaseMap.get(hbase));
		}
		return historyMap;
	}
	
	/**
	 * 获取配置的列映射
	 * @param col
	 * @return
	 */
	public static Map<String,String> getColumnName(Column col) {
		String header = col.getHeader();
		String mysql = col.getMysql();
		String hbase = col.getHbase();
		Map<String,String> map = new HashMap<String,String>();
		//有header
		if(header != null && !"".equals(header.trim())){
			map.put("header", header);
		} 
		//有mysql
		if(mysql != null && !"".equals(mysql.trim())){
			map.put("mysql", mysql);
		} 
		//有hbase
		if(hbase != null && !"".equals(hbase.trim())){
			map.put("hbase", hbase);
		}
		return map;
	}

	/**
	 * 未明确配置Column元素时，默认同步所有列，列名的前缀使用表的前缀
	 * @param columnPrefix
	 * @param colname
	 * @param json
	 * @return
	 */
	public static Tuple2<Map<String,String>,Map<String,String>> getColumnValue(String columnPrefix, String name, JSONObject json) {
		//是否使用前缀，值为null或false，不使用前缀
		String value = null;
		Map<String, String> colMap = new HashMap<String, String>();
		Map<String, String> originalColMap = new HashMap<String, String>();
		JSONObject columns = json.getJSONObject("columns");
		if(columns == null){
			throw new SyncException("接收的数据中columns的值为空！");
		}
		value = columns.getString(name);
		String orginalName = name;
		originalColMap.put(orginalName, value);
		//如果未配置column元素，所有列在hbase中的实际名称都按前缀处理
		if(columnPrefix != null && !"".equals(columnPrefix.trim())){
			name = columnPrefix + name;
		}
		colMap.put(name, value);
		return new Tuple2<Map<String,String>,Map<String,String>>(originalColMap,colMap);
	}
	
	/**
	 * 获取列与值的映射
	 * @param col
	 * @return
	 */
	public static Tuple2<Map<String,String>,Map<String,String>> getColumnValue(String columnPrefix, Column col, JSONObject json) {
		
		Map<String,String> nameMap = getColumnName(col);
		String header = nameMap.get("header");
		String mysql = nameMap.get("mysql");
		String hbase = nameMap.get("hbase");
		String handlerClass = col.getHandlerClass();
		
		//是否使用前缀，值为null或false，不使用前缀
		Boolean prefix = col.getPrefix();
		
		String value = col.getValue();
		Map<String, String> colMap = new HashMap<String, String>();
		Map<String, String> originalColMap = new HashMap<String, String>();
		JSONObject columns = json.getJSONObject("columns");
		//mysql列名称
		String originalName = "";
		//hbase列名称
		String name = "";
		
		//非常量配置处理
		if(value == null || "".equals(value.trim())){
			/*
			 * 配置情况: 1.只有header, 2.只有mysql, 3.只有hbase, 4.header和hbase, 5.mysql和hbase
			 * 6.有handlerClass
			 */
			//1.只有header, 与header和hbase一致
			if(header != null && !"".equals(header) && mysql == null && hbase == null){
				originalName = header;
				value = json.getString(header);
				name = header;
			}
			//2.只有mysql, 与mysql和hbase一致
			if(mysql != null && !"".equals(mysql) && header == null && hbase == null){
				originalName = mysql;
				value = columns.getString(mysql);
				name = mysql;
			}
			//3.只有hbase, 与mysql和hbase一致，默认不与header一致
			if(hbase != null && !"".equals(hbase) && header == null && mysql == null){
				originalName = hbase;
				value = columns.getString(hbase);
				name = hbase;
			}
			//4.header和hbase, header和hbase映射
			if(hbase != null && header != null && mysql == null){
				originalName = header;
				value = json.getString(header);
				name = hbase;
			}
			//5.mysql和hbase, mysql和hbase映射
			if(hbase != null && mysql != null && header == null){
				originalName = mysql;
				value = columns.getString(hbase);
				name = hbase;
			}
			//6.有handlerClass
			if(handlerClass != null && !"".equals(handlerClass.trim())){
				Map<String,String> map = null;
				if(header != null && !"".equals(header)){
					//如果配置的是header，则从接收的json数据中取属性值
					map = getHandlerValue(originalName, json, handlerClass);
				} else {
					//如果配置的不是header，则从接收json的子对象columns中取值
					map = getHandlerValue(originalName, columns, handlerClass);
				}
				value = map.get(originalName);
			} else {
				//如果没有handlerClass属性，则根据配置属性映射取值
				if(header != null && !"".equals(header)){
					//如果配置的是header，则从接收的json数据中取属性值
					value = json.getString(originalName);
				} else {
					//如果配置的不是header，则从接收json的子对象columns中取值
					value = columns.getString(originalName);
				}
			}
		} else {
			//常量配置处理，originalName没有意义,但设置成和name一致
			name = col.getHbase();
			originalName = name;
			if(name == null || "".equals(name)){
				throw new SyncException("常量配置列的hbase属性不能为空!");
			}
		}
		
		
		originalColMap.put(originalName, value);
		//如果有前缀
		if(columnPrefix != null && !"".equals(columnPrefix)){
			//前缀处理，未配置prefix时，根据columnPrefix加入前缀
			if(prefix == null || "".equals(prefix) || prefix){
				name = columnPrefix + name;
			} else if(!prefix){
				//为false时，不加入前缀，所以不处理
			} else {
				throw new ConfigureException("prefix属性配置不合法！请查看：prefix=\"" + prefix + "\"");
			}
		}
		colMap.put(name, value);
		Tuple2<Map<String,String>,Map<String,String>> tuple = 
				new Tuple2<Map<String,String>,Map<String,String>>(originalColMap, colMap);
		return tuple;
	}
	
	//根据handler处理，产并返回值
	private static Map<String,String> getHandlerValue(String name, JSONObject json, String handlerClass) {
		//如果有handlerClass,则用类处理，处理类配置优先处理
		if(handlerClass != null && !"".equals(handlerClass)){
			Convertible convertor = null;
			try {
				convertor = (Convertible)Class.forName(Constants.HANDLER_CLASS_PACKAGE + "." + handlerClass).newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				throw new SyncException(e, "实例化处理类失败！"
						+ "请查看handlerClass配置是否正确：handlerClass=\"" + handlerClass + "\"");
			}
			return convertor.getValue(name, json);
		} else {
			throw new SyncException("规则处理错误：" + handlerClass + "不存在！");
		}
	}
	
	/**
	 * 获取条件
	 * @param cols
	 * @param json
	 * @return
	 */
	public static boolean getCriterias(List<Column> cols, JSONObject json){
		Map<String,String> mustMap = new HashMap<String,String>();
		Map<String,String> shouldMap = new HashMap<String,String>();
		Map<String,String> notMap = new HashMap<String,String>();
		for(Column col : cols){
			String header = col.getHeader();
			String mysql = col.getMysql();
			String value = col.getValue();
			String require = col.getRequire();
			if(require.equals(RequireType.MUST.getValue())){
				if(header != null && !"".equals(header)){
					mustMap.put(header + "###" + "header", value);
				}
				if(mysql != null && !"".equals(mysql)){
					mustMap.put(mysql + "###" + "mysql", value);
				}
			}
			if(require.equals(RequireType.SHOULD.getValue())){
				if(header != null && !"".equals(header)){
					shouldMap.put(header + "###" + "header", value);
				}
				if(mysql != null && !"".equals(mysql)){
					shouldMap.put(mysql + "###" + "mysql", value);
				}
			}
			if(require.equals(RequireType.NOT.getValue())){
				if(header != null && !"".equals(header)){
					notMap.put(header + "###" + "header", value);
				}
				if(mysql != null && !"".equals(mysql)){
					notMap.put(mysql + "###" + "mysql", value);
				}
			}
		}
		//条件判断
		boolean mustFlag = checkColumns(json, mustMap, RequireType.MUST);
		boolean shouldFlag = checkColumns(json, shouldMap, RequireType.SHOULD);
		boolean notFlag = checkColumns(json, notMap, RequireType.NOT);
		return mustFlag && notFlag && shouldFlag;
	}

	/**
	 * 检查列条件
	 * @param json
	 * @param mustMap
	 * @param requireType
	 * @return
	 */
	private static boolean checkColumns(JSONObject json,
			Map<String, String> mustMap, RequireType requireType) {
		boolean andflag = true;
		boolean orflag = false;
		boolean notflag = true;
		Set<String> mustSet = mustMap.keySet();
		for(String key : mustSet){
			boolean flag = false;
			String [] names = key.split("###");
			String name = names[0];
			String type = names[1];
			String value = mustMap.get(key);
			if(type.equals("header")){
				if(value.equals(json.getString(name))){
					if(requireType.equals(RequireType.NOT.getValue())){
						flag = false;
					} else {
						flag = true;
					}
				} else {
					if(requireType.equals(RequireType.NOT.getValue())){
						flag = true;
					} else {
						flag = false;
					}
				}
			}
			if(type.equals("mysql")){
				JSONObject columns = json.getJSONObject("columns");
				if(value.equals(columns.getString(name))){
					if(requireType.equals(RequireType.NOT.getValue())){
						flag = false;
					} else {
						flag = true;
					}
				} else {
					if(requireType.equals(RequireType.NOT.getValue())){
						flag = true;
					} else {
						flag = false;
					}
				}
			}
			if(requireType.equals(RequireType.MUST.getValue())){
				andflag = andflag && flag;
			}
			if(requireType.equals(RequireType.SHOULD.getValue())){
				orflag = orflag || flag;
			}
			if(requireType.equals(RequireType.NOT.getValue())){
				notflag = notflag && flag;
			}
			
		}
		if(requireType.equals(RequireType.MUST.getValue())){
			return andflag;
		}
		if(requireType.equals(RequireType.SHOULD.getValue())){
			return orflag;
		}
		if(requireType.equals(RequireType.NOT.getValue())){
			return notflag;
		}
		return false;
	}

	/**
	 * 验证配置条件
	 * @param tableNode
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static boolean checkCriteria(ConfigTable tableNode,JSONObject value)throws Exception{
		boolean flag = false;
		//取kafka中返回column值
		JSONObject columnsTmp = value.getJSONObject("columns");
		Criteria cirteria = tableNode.getCriteria();
		if(cirteria != null){
			List<Column> criteriaCols = cirteria.getColumns();
			if(criteriaCols != null && criteriaCols.size()>0){
				//遍历条件列表
				for(int i = 0; i < criteriaCols.size() ; i++){
					Column col = criteriaCols.get(i);
					String name = col.getMysql();
					String val = col.getValue();
					//如果条件值不为空，并且对应的数据字段值与该配置值相符，则条件为true
					if(val != null && val.equals(columnsTmp.get(name))){
						//如果第一个条件符合，则设置为true
						flag = true;
					} else {
						//如果值为空，或数据中字段值不相符，则设置为false
						flag = false;
						break;
					}
				}
			}else{
				//如果没有配置条件列表，返回true
				flag = true;
			}
			return flag;
		}
		//如果没有配置条件列表，返回true
		return true;
	}
	
	/**
	 * 获取列映射，根据配置和数据组装成最终列映射的Map。
	 * Map中存放要存储的列名和对应的列值
	 * @param tableNode 配置中的表节点
	 * @param value	JSON数据
	 * @return Tuple2<Map<String,String>,Map<String,String>> 返回mysql字段与值的Map和hbase字段与值的映射
	 * @throws Exception
	 */
	public static Tuple2<Map<String,String>,Map<String,String>> getColumnMapped(ConfigTable tableNode,JSONObject value)throws Exception{
		//最终返回的列映射列表
		Map<String,String> colsMapping = new HashMap<String,String>();
		//原始列和值
		Map<String,String> originalColsMapping = new HashMap<String,String>();
		//配置中的列名与列的映射，不配置时，得到的将是空的map
		Map<String,List<Column>> columnsMap = tableNode.getColumnsMap();
		//前缀
		String columnPrefix = tableNode.getColumnPrefix();
		String scope = tableNode.getScope();
		//取kafka中返回columns值
		JSONObject jsonColumns = value.getJSONObject("columns");
		if(jsonColumns == null || jsonColumns.size() == 0){
			throw new SyncException("columns数据错误（columns为空对象）：", value);
		}
		//如果scope为空, 则同步所有列
		if(scope==null || "".equals(scope)){
			Set<String> keySet = jsonColumns.keySet();
			for(String key : keySet){
				//返回所有列的映射
				Tuple2<Map<String,String>,Map<String,String>> colMaps = ConfigureUtil.getColumnValue(columnPrefix, key, value);
				Map<String, String> orginalColMap = colMaps._1;
				Map<String, String> colMap = colMaps._2;
				colsMapping.putAll(colMap);
				originalColsMapping.putAll(orginalColMap);
			}
			Set<String> set = columnsMap.keySet();
			for(String key : set) {
				List<Column> colList = columnsMap.get(key);
				for (Column col : colList) {
					//组装成带前缀的列
					Tuple2<Map<String,String>,Map<String,String>> colMaps = ConfigureUtil.getColumnValue(columnPrefix, col, value);
					Map<String, String> orginalColMap = colMaps._1;
					Map<String, String> colMap = colMaps._2;
					colsMapping.putAll(colMap);
					originalColsMapping.putAll(orginalColMap);
				}
			}
		}else if("specified".equals(scope)){
			//设置mysql的所有列值，在这里如果只用指定配置的字段，可能得不到未配置的字段的值
			Set<String> jsonSet = jsonColumns.keySet();
			for(String key : jsonSet){
				//返回所有列的映射
				Tuple2<Map<String,String>,Map<String,String>> colMaps = ConfigureUtil.getColumnValue(columnPrefix, key, value);
				Map<String, String> orginalColMap = colMaps._1;
				originalColsMapping.putAll(orginalColMap);
			}
			//如果scope="specified"则按配置的列进行同步,只同步指定的column字段
			Set<String> keySet = columnsMap.keySet();
			for(String key : keySet) {
				List<Column> colList = columnsMap.get(key);
				for (Column col : colList) {
					//组装成带前缀的列
					Tuple2<Map<String,String>,Map<String,String>> colMaps = ConfigureUtil.getColumnValue(columnPrefix, col, value);
					Map<String, String> orginalColMap = colMaps._1;
					Map<String, String> colMap = colMaps._2;
					colsMapping.putAll(colMap);
					originalColsMapping.putAll(orginalColMap);
				}
			}
		} else {
			throw new ConfigureException("table元素中scope配置不合法！请检查元素: "
					+ " table[mysql=\"" + tableNode.getMysql() + "\" hbase=\"" 
					+ tableNode.getHbase() + "\" scope=\"" + scope + "\"]");
		}
		colsMapping.remove("executeTime");
		colsMapping.put("T_LAST_TIMESTAMP", String.valueOf(new Date().getTime()));
		colsMapping.put((("".equals(columnPrefix) || null == columnPrefix ? "T_":columnPrefix)+"TIMESTAMP").toUpperCase(),value.getString("executeTime"));
		Tuple2<Map<String,String>,Map<String,String>> tuple2 = new Tuple2<Map<String,String>,Map<String,String>>(originalColsMapping,colsMapping);
		return tuple2;
	}
	
	/**
	 * 根据配置获取rowkey字符串
	 * @param tableNode
	 * @param value
	 * @param columnsAfterMapping
	 * @return
	 * @throws Exception
	 */
	public static String getRowkey(String key, ConfigTable tableNode, Tuple2<Map<String,String>,Map<String,String>> tuple)throws Exception{
		String rowkey = getColumnKey(null, "rowkey", tableNode,tuple);
		String hashkey = getColumnKey(key, "hashkey", tableNode,tuple);
		//如果配置了hashkey，则进行hash
		if(hashkey != null && !"".equals(hashkey)){
			hashkey = DigestUtils.md5Hex(hashkey);
			rowkey = hashkey.substring(0,4) + "_" + rowkey;
		}
		return rowkey;
	}
	
	/**
	 * 根据类型获取rowkey或hashkey的组合值
	 * @param type
	 * @param tableNode
	 * @param value
	 * @param columnsMapping
	 * @return
	 */
	public static String getColumnKey(String key, String type, ConfigTable tableNode, Tuple2<Map<String,String>,Map<String,String>> tuple){
		String pk = "";
		//生成hashkey,前四位用于rowkey前缀
		Identifiable keys = null;
		if("rowkey".equals(type)){
			keys = tableNode.getRowkeys();
		} else if("hashkey".equals(type)){
			keys = tableNode.getHashkeys();
		}
		//如果配置了rowkey或hashkey元素
		if(keys != null){
			List<Column> cols = keys.getColumns();
			//如果有column子元素
			if(cols != null && cols.size() > 0){
				for(Column col : cols){
					String mysql = col.getMysql();
					String hbase = col.getHbase();
					String colName = mysql;
					if(mysql == null || "".equals(mysql)){
						colName = hbase;
						if(!"".equals(pk)){
							pk += "_";
						}
						String value = tuple._2.get(colName);
						pk += value == null || "".equals(value) ? "" : value;
					} else {
						if(!"".equals(pk)){
							pk += "_";
						}
						String value = tuple._1.get(colName);
						pk += value == null || "".equals(value) ? "" : value;
					}
				}
			} else {
				if("hashkey".equals(type)){
					if(key == null || "".equals(key)){
						throw new SyncException("接收到hashkey字段异常！key=" + key);
					}
					//如果没有子元素，则使用接收数据的key值
					pk = key;
				}
			}
		} 
		return pk;
	}
	
	public static String toJSONString(TagElement el){
		JSONObject json = new JSONObject();
		Class<? extends TagElement> clazz = el.getClass();
		Field []  fields = clazz.getDeclaredFields();
		try {
			for(Field field : fields){
				String name = field.getName();
				String mname = "get" + name.substring(0,1).toUpperCase() + name.substring(1);
				Method method = clazz.getDeclaredMethod(mname);
				if(method != null){
					Object obj = method.invoke(el);
					json.put(name, obj.toString());
				}
			}
		} catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}
		return json.toJSONString();
	}

	public static void validateTest(String name) {
		StorageConfig config = ConfigureUtil.loadConfigure("storageConfig.xml");
		Map<String,ConfigTable> singleTableMap = config.getSingleTableMap();
		Map<String,List<ConfigTable>> tableMap = config.getTableMap();
		Map<String,ConfigTable> historyMap = config.getHistoryMap();
		List<String> aggList = config.getAggregations();
		
		System.out.println("------------------------------全表");
		//所有表
		for(String key : tableMap.keySet()){
			List<ConfigTable> ls = tableMap.get(key);
			for(ConfigTable t : ls){
				System.out.println(key + " : " + t.getMysql() + "-->" + t.getHbase() + "[" + t.getHbaseHist() + "]");
				HashKeys hks = t.getHashkeys();
				if(hks != null){
					List<Column> cols = hks.getColumns();
					System.out.println("hashkey-------------------");
					for(Column col : cols){
						System.out.println(col.getMysql() + "=>" + col.getHbase());
					}
				}
				RowKeys rks = t.getRowkeys();
				if(rks != null){
					System.out.println("rowkey-------------------");
					List<Column> cols = rks.getColumns();
					for(Column col : cols){
						System.out.println(col.getMysql() + "=>" + col.getHbase());
					}
				}
			}
		}
		System.out.println("------------------------------单表");
		//单表
		for(String key : singleTableMap.keySet()){
			ConfigTable t = singleTableMap.get(key);
			System.out.println(key + " : " + t.getMysql() + "-->" + t.getHbase() + "[" + t.getHbaseHist() + "]");
		}
		System.out.println("------------------------------历史表");
		for(String key : historyMap.keySet()){
			ConfigTable t = historyMap.get(key); 
			System.out.println(key + " : " + ((t != null) ? t.getMysql() + "-->" + t.getHbase() + "[" + t.getHbaseHist() + "]" : ""));
			/*if(t != null){
				System.out.println(t.getMysql() + "-->" + t.getHbase() + "[" + t.getHbaseHist() + "]");
			}*/
		}
		System.out.println("------------------------------聚合表");
		for(String t : aggList){
			System.out.println(t);
		}
		
		
		String tableName = "DATA_YPT_TRAN";
		System.out.println("测试--------------------");
		System.out.println(canDelete(config, tableName, historyMap));
	}
	
	private static boolean canDelete(StorageConfig config, String tableName,
			Map<String, ConfigTable> historyMap) {
		//默认不删除
		boolean flag = false;
		ConfigTable table = config.getSingleTableMap().get(tableName);
		ConfigTable t = null;
		if(table != null){
			String hbase = table.getHbaseHist();
			t = historyMap.get(hbase);	
		}
		if(t == null){
			return true;
		}
		System.out.println(((t != null) ? t.getMysql() + "-->" + t.getHbase() + "[" + t.getHbaseHist() + "]" : ""));
		return false;
	}
	
	
	public static void main(String[] args) {
		//validateTest(null);
		//testHashKey();
		
		String hashkey = DigestUtils.md5Hex("00201803281402323272");
		System.out.println(hashkey);
	}

	private static void testHashKey() {
		ConfigTable tableNode = new ConfigTable();
		HashKeys hks = new HashKeys();
		List<Column> hkList = new ArrayList<Column>();
		Column hk = new Column();
		hk.setMysql("METRO_MEMBER_CARD_NUM");
		hkList.add(hk);
		hks.setColumns(hkList);
		tableNode.setHashkeys(hks);
		
		RowKeys rks = new RowKeys();
		List<Column> rkList = new ArrayList<Column>();
		Column rk1 = new Column();
		rk1.setMysql("DEBIT_REQUEST_ORDER_NO");
		Column rk2 = new Column();
		rk2.setHbase("CITY_CODE");
		rkList.add(rk1);
		rkList.add(rk2);
		rks.setColumns(rkList);
		tableNode.setRowkeys(rks);
		Map<String,String> om = new HashMap<String,String>();
		om.put("DEBIT_REQUEST_ORDER_NO", "05201803210019459572");
		om.put("METRO_MEMBER_CARD_NUM", "0180802006342943");
		Map<String,String> nm = new HashMap<String,String>();
		nm.put("CITY_CODE", "4401");
		Tuple2<Map<String,String>,Map<String,String>> tuple = 
				new Tuple2<Map<String,String>,Map<String,String>>(om,nm);
		String rowkey = getColumnKey(null, "rowkey", tableNode, tuple);
		String hashkey = getColumnKey("0180802006342943", "hashkey", tableNode,tuple);
		hashkey = DigestUtils.md5Hex(hashkey);
		rowkey = hashkey.substring(0,4) + "_" + rowkey;
		System.out.println(rowkey);
	}
	
}
