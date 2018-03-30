package com.shankephone.data.storage.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.BitComparator.BitwiseOp;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {
	private Configuration conf = null;
	private Connection conn = null;
	private Admin admin = null;
	
	/**
	 * @todo 初始化连接
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月13日 下午5:41:27
	 */
	@Before
	public void init() throws Exception{
		conf = HBaseConfiguration.create();
		//TODO 需更换为自己的zookeeper集群
		//对于hbase客户端来说，只需要知道hbase使用的zookeeper集群地址就可以了，hbase客户端读写数据不需要知道hmaster
//		conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop06:2181,hadoop07:2181");
		conf.set("hbase.zookeeper.quorum", "data1.test:2181,data2.test:2181,data3.test:2181,data4.test:2181,data5.test:2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
		//获取连接
		conn = ConnectionFactory.createConnection(conf);
		//获取一个表管理器
		admin = conn.getAdmin();
	}
	
	/**
	 * @todo 创建表
	 * @author sunweihong
	 * 2017年4月13日 下午5:42:07
	 * @throws Exception 
	 */
	@Test
	public void createTable() throws Exception{
		
		//构建表描述器
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("skp_user_info"));
		//构建列族描述器
		HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
		//设置布隆过滤器
		hcd1.setBloomFilterType(BloomType.ROW).setMinVersions(1).setMaxVersions(3);
		
		//构建列族描述器
		HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info");
		//设置布隆过滤器
		hcd2.setBloomFilterType(BloomType.ROW).setMinVersions(1).setMaxVersions(3);
		
		htd.addFamily(hcd1).addFamily(hcd2);
		
		admin.createTable(htd);
	}
	
	/**
	 * @todo 修改表
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 上午10:36:17
	 */
	@Test
	public void modifyTable() throws Exception{
		//通过admin获取需要的表描述器
		HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf("skp_user_info"));
		//通过表描述器获取列族描述器
		HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes("extra_info"));
		//设置新的Bloom过滤器
		hcd.setBloomFilterType(BloomType.ROWCOL);
		//添加新的列族
		htd.addFamily(new HColumnDescriptor("other_info"));
		//修改表
		admin.modifyTable(TableName.valueOf("skp_user_info"), htd);
		
	}
	
	/**
	 * @todo 插入数据
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 上午10:36:26
	 */
	@Test
	public void testPut() throws Exception{
		
		Table table = conn.getTable(TableName.valueOf("skp_user_info"));
		List<Put> puts = new ArrayList<Put>();
		//key
		Put put01 = new Put(Bytes.toBytes("user001"));
		//value
		put01.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		
		Put put02 = new Put(Bytes.toBytes("user001"));
		put02.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));
		
		Put put03 = new Put(Bytes.toBytes("user002"));
		put03.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
		put03.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));
		
		Put put04 = new Put(Bytes.toBytes("zhang_sh_01"));
		put04.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang01"));
		put04.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

		Put put05 = new Put(Bytes.toBytes("zhang_sh_02"));
		put05.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang02"));
		put05.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

		Put put06 = new Put(Bytes.toBytes("liu_sh_01"));
		put06.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("liu01"));
		put06.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));
		put06.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));

		Put put07 = new Put(Bytes.toBytes("zhang_bj_01"));
		put07.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang03"));
		put07.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));
		
		Put put08 = new Put(Bytes.toBytes("zhang_bj_01"));
		put08.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang04"));
		put08.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));
		
		puts.add(put01);
		puts.add(put02);
		puts.add(put03);
		puts.add(put04);
		puts.add(put05);
		puts.add(put06);
		puts.add(put07);
		puts.add(put08);

		table.put(puts);
		
	}
	
	/**
	 * @todo 获取
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 上午10:50:05
	 */
	@Test
	public void testGet() throws Exception{
		
		Table table = conn.getTable(TableName.valueOf("skp_user_info"));
		//获取row
		Result result = table.get(new Get(Bytes.toBytes("user001")));
		//循环里面的column
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()){
			Cell currentCell = cellScanner.current();
			byte[] familyArray = currentCell.getFamilyArray();
			byte[] qualifierArray = currentCell.getQualifierArray();
			byte[] valueArray = currentCell.getValueArray();
			
//			System.out.println(new String(familyArray));
//			System.out.println(new String(qualifierArray));
//			System.out.println(new String(valueArray));
			
			String formatString = new String(familyArray,currentCell.getFamilyOffset(),currentCell.getFamilyLength()) +":"
							+new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength())+ " "
							+new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength());
							
			System.out.println(formatString);
		}
		
	}
	
	/**
	 * @todo 批量获取数据
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 上午11:02:08
	 */
	@Test
	public void testScan() throws Exception{
		Table table = conn.getTable(TableName.valueOf("skp_user_info"));
		//scan搜索的列含首不含尾，所以在后面加上"\000"
		ResultScanner scanner = table.getScanner(new Scan(Bytes.toBytes("user001"),Bytes.toBytes("user002"+"\000")));
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
//				System.out.println(new String(familyArray));
//				System.out.println(new String(qualifierArray));
//				System.out.println(new String(valueArray));
				
				String formatString = new String(rowArray, currentCell.getRowOffset(), currentCell.getRowLength()) +" > "+
								new String(familyArray,currentCell.getFamilyOffset(),currentCell.getFamilyLength()) +":"
								+new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength())+ " "
								+new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength());
								
				System.out.println(formatString);
			}
		}
		table.close();
	}
	
	/**
	 * @todo 分页查询
	 * @author sunweihong
	 * 2017年4月14日 上午11:36:17
	 * @throws Exception 
	 */
	@Test
	public void testPageScan() throws Exception{
		final byte[] POSTFIX = new byte[] { 0x00 }; 
		Table table = conn.getTable(TableName.valueOf("skp_user_info"));
		Filter filter = new PageFilter(3);   // 一次需要获取一页的条数
		byte[] lastRow = null;  
		int totalRows = 0;  
		while (true) {  
		    Scan scan = new Scan();  
		    scan.setFilter(filter);  
		    if(lastRow != null){  
		        byte[] startRow = Bytes.add(lastRow,POSTFIX);   //设置本次查询的起始行键  
		        scan.setStartRow(startRow);  
		    }  
		    ResultScanner scanner = table.getScanner(scan); 
		    int localRows = 0;  
		    Result result;  
		    while((result = scanner.next()) != null){  
		        System.out.println(++localRows + ":" + result); 
		        totalRows ++;  
		        lastRow = result.getRow();  
		    }  
		    scanner.close();  
		    if(localRows == 0) break; 
		    Thread.sleep(2000);
		}  
		System.out.println("total rows:" + totalRows);
		table.close();
	}
	

	/**
	 * @todo 使用过滤器筛选出具有特定前缀的行键的数据
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testPrefixFilter() throws Exception {
		PrefixFilter columnCountGetFilter = new PrefixFilter(Bytes.toBytes("zhang"));
		testScan(columnCountGetFilter);
	}
	
	/**
	 * @todo 使用过滤器筛选符合条件的key的row，只能筛选key
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testRowFilter() throws Exception {
		/*		
		 * CompareOp:比较运算符
		 * ByteArrayComparable:比较器的基础类，其子类主要包括一下几个
		 * 	BinaryComparator：二进制比较器，按照字典顺序比较 
		 * 	BinaryPrefixComparator：二进制前缀比较器，按照字典顺序比较 
		 * 	BitComparator：位比较器
		 * 	LongComparator：整数比较器
		 * 	NullComparator：空比较器
		 * 	RegexStringComparator：正则表达式比较器
		 * 	SubstringComparator：子字符串比较器
		 */
		RowFilter rf = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("user001"))); //筛选出key=user001的row
//		RowFilter rf = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("user"))); //筛选出key前缀为user的row
//		RowFilter rf = new RowFilter(CompareOp.EQUAL, new LongComparator(0)); 
//		RowFilter rf = new RowFilter(CompareOp.EQUAL, new NullComparator()); 
//		RowFilter rf = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d+$")); 
//		RowFilter rf = new RowFilter(CompareOp.EQUAL, new SubstringComparator("001"));
		testScan(rf);
	}
	
	/**
	 * @todo 通过单个column的值筛选需要的行column
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testSingleColumnValueFilter() throws Exception {
		/*
		 * family:列族
		 * qualifier:column标识
		 * CompareOp:比较运算符
		 * ByteArrayComparable:比较器的基础类，其子类主要包括一下几个
		 * 	BinaryComparator：二进制比较器，按照字典顺序比较 
		 * 	BinaryPrefixComparator：二进制前缀比较器，按照字典顺序比较 
		 * 	BitComparator：位比较器
		 * 	LongComparator：整数比较器
		 * 	NullComparator：空比较器
		 * 	RegexStringComparator：正则表达式比较器
		 * 	SubstringComparator：子字符串比较器
		 * */
		SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("password"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("123456"))); 
		scvf.setFilterIfMissing(true);   // 如果指定的列缺失，则也过滤掉
		scvf.setLatestVersionOnly(true); //只取最新数据
		testScan(scvf);
	}
	/**
	 * @todo 通过列筛名筛选其下所有的column
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testFamilyFilter() throws Exception {
		/*
		 * CompareOp:比较运算符
		 * ByteArrayComparable:比较器的基础类，其子类主要包括一下几个
		 * 	BinaryComparator：二进制比较器，按照字典顺序比较 
		 * 	BinaryPrefixComparator：二进制前缀比较器，按照字典顺序比较 
		 * 	BitComparator：位比较器
		 * 	LongComparator：整数比较器
		 * 	NullComparator：空比较器
		 * 	RegexStringComparator：正则表达式比较器
		 * 	SubstringComparator：子字符串比较器
		 * */
		FamilyFilter ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("base_info")));  
		testScan(ff);
	}
	
	/**
	 * @todo 通过筛名筛选的column
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testQualifierFilter() throws Exception {
		/*
		 * CompareOp:比较运算符
		 * ByteArrayComparable:比较器的基础类，其子类主要包括一下几个
		 * 	BinaryComparator：二进制比较器，按照字典顺序比较 
		 * 	BinaryPrefixComparator：二进制前缀比较器，按照字典顺序比较 
		 * 	BitComparator：位比较器
		 * 	LongComparator：整数比较器
		 * 	NullComparator：空比较器
		 * 	RegexStringComparator：正则表达式比较器
		 * 	SubstringComparator：子字符串比较器
		 * */
		QualifierFilter qf = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("us")));
		testScan(qf);
	}
	
	/**
	 * @todo 通过列名筛选其下所有的column的前缀为passw的值，只返回符合的value
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testColumnPrefixFilter() throws Exception {
		ColumnPrefixFilter cf = new ColumnPrefixFilter(Bytes.toBytes("passw"));
		testScan(cf);
	}
	
	/**
	 * @todo 通过列名筛选其下所有的column的前缀为username和passw的值，只返回符合的value
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testMultipleColumnPrefixFilter() throws Exception {
		byte[][] prefixes = new byte[][] { Bytes.toBytes("username"),Bytes.toBytes("password") };
		MultipleColumnPrefixFilter mcf = new MultipleColumnPrefixFilter(prefixes);
		testScan(mcf);
	}
	
	/**
	 * @todo 通过多个过滤器筛选符合的值
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月19日 上午9:52:58
	 */
	@Test
	public void testFilterList() throws Exception {
		/*
		 * Operator:逻辑标志
		 * MUST_PASS_ALL=AND
		 * MUST_PASS_ONE=OR
		 * */
//		获取列族前缀为“base”，列名前缀为“passw”的列的值
//		FamilyFilter ff2 = new FamilyFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("base")));
//		ColumnPrefixFilter cf = new ColumnPrefixFilter("passw".getBytes());
//		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
//		filterList.addFilter(ff2);
//		filterList.addFilter(cf);
		
//		获取base_info:sex=female and base_info:age=20 的行
//		SingleColumnValueFilter scvf1 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("female"))); 
//		scvf1.setFilterIfMissing(true);
//		scvf1.setLatestVersionOnly(true);
//		SingleColumnValueFilter scvf2 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("age"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("20"))); 
//		scvf2.setFilterIfMissing(true);
//		scvf2.setLatestVersionOnly(true);
//		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
//		filterList.addFilter(scvf1);
//		filterList.addFilter(scvf2);
		
		//查询(base_info:sex=female and base_info:age=20) or (base_info:sex=male and extra_info:married=false) 的行
		SingleColumnValueFilter scvf1 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("female"))); 
		scvf1.setFilterIfMissing(true);
		scvf1.setLatestVersionOnly(true);
		SingleColumnValueFilter scvf2 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("age"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("20"))); 
		scvf2.setFilterIfMissing(true);
		scvf2.setLatestVersionOnly(true);
		FilterList filterList1 = new FilterList(Operator.MUST_PASS_ALL);
		filterList1.addFilter(scvf1);
		filterList1.addFilter(scvf2);
		FilterList filterList2 = new FilterList(Operator.MUST_PASS_ALL);
		SingleColumnValueFilter scvf3 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("male"))); 
		scvf3.setFilterIfMissing(true);
		scvf3.setLatestVersionOnly(true);
		SingleColumnValueFilter scvf4 = new SingleColumnValueFilter(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("false"))); 
		scvf4.setFilterIfMissing(true);
		scvf4.setLatestVersionOnly(true);
		filterList2.addFilter(scvf3);
		filterList2.addFilter(scvf4);
		
		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
		filterList.addFilter(filterList1);
		filterList.addFilter(filterList2);
		
		testScan(filterList);
	}
	
	
	public void testScan(Filter filter) throws Exception {

		Table t_user_info = conn.getTable(TableName.valueOf("skp_user_info"));

		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = t_user_info.getScanner(scan);

		Iterator<Result> iter = scanner.iterator();
		while (iter.hasNext()) {
			Result result = iter.next();
			CellScanner cellScanner = result.cellScanner();
			while (cellScanner.advance()) {
				Cell current = cellScanner.current();
				byte[] familyArray = current.getFamilyArray();
				byte[] valueArray = current.getValueArray();
				byte[] qualifierArray = current.getQualifierArray();
				byte[] rowArray = current.getRowArray();

				System.out.print(new String(rowArray, current.getRowOffset(), current.getRowLength())+"   ");
				System.out.print(new String(familyArray, current.getFamilyOffset(), current.getFamilyLength()));
				System.out.print(":" + new String(qualifierArray, current.getQualifierOffset(), current.getQualifierLength()));
				System.out.println(" " + new String(valueArray, current.getValueOffset(), current.getValueLength()));
			}
			System.out.println("--------------------------------------------------------------------");
		}
	}
	
	/**
	 * @todo 删除数据
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 下午12:04:31
	 */
	@Test
	public void testDel() throws Exception {

		Table table = conn.getTable(TableName.valueOf("skp_user_info"));
		Delete delete = new Delete("user001".getBytes());
		delete.addColumn("base_info".getBytes(), "password".getBytes());
		table.delete(delete);
		table.close();
	}
	
	@Test
	public void testRowkey() throws Exception{
		Table table = conn.getTable(TableName.valueOf("skp:t_taste_order"));
		Delete delete = new Delete("403d_c41734618a1d4d87b5a84bedfbb9bf14".getBytes());
		table.delete(delete);
		table.close();
		System.out.println("cc");
	}
	
	/**
	 * @todo 删除表
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月14日 下午12:04:57
	 */
	@Test
	public void testDrop() throws Exception {
		admin.disableTable(TableName.valueOf("skp_user_info"));
		admin.deleteTable(TableName.valueOf("skp_user_info"));
	}
	
	
	@After
	public void close() throws Exception{
		admin.close();
		conn.close();
	}

}
