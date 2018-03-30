package com.shankephone.data.storage.hbase;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.shankephone.data.common.hbase.HBaseClient;
import com.shankephone.data.common.test.SpringTestCase;

public class HbaseConnectTest extends SpringTestCase {
	
	private HBaseClient hc = null;

	@Before
	public void getConnect(){
		hc = HBaseClient.getInstance();
	}
	@Test
	public void getRowsByFilters(){
//		SingleColumnValueFilter scvf1 = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("OPT_EVENT"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("1"))); 
//		scvf1.setFilterIfMissing(true);
//		scvf1.setLatestVersionOnly(true);
//		SingleColumnValueFilter scvf2 = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("OPT_RESULT"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("SUCCESS"))); 
//		scvf2.setFilterIfMissing(true);
//		scvf2.setLatestVersionOnly(true);
//		FilterList filterList1 = new FilterList(Operator.MUST_PASS_ALL);
//		filterList1.addFilter(scvf1);
//		filterList1.addFilter(scvf2);
//		FilterList filterList2 = new FilterList(Operator.MUST_PASS_ALL);
//		SingleColumnValueFilter scvf3 = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("ORDER_SOURCE"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("02"))); 
//		scvf3.setFilterIfMissing(true);
//		scvf3.setLatestVersionOnly(true);
//		SingleColumnValueFilter scvf4 = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("PAY_STATUS"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("SUCCESS"))); 
//		scvf4.setFilterIfMissing(true);
//		scvf4.setLatestVersionOnly(true);
//		filterList2.addFilter(scvf3);
//		filterList2.addFilter(scvf4);
//		
//		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
//		filterList.addFilter(filterList1);
//		filterList.addFilter(filterList2);
		
		SingleColumnValueFilter scvf1 = new SingleColumnValueFilter(Bytes.toBytes("D"), Bytes.toBytes("ORDER_TYPE"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("TICKET"))); 
		scvf1.setFilterIfMissing(true);
		scvf1.setLatestVersionOnly(true);
		hc.getRowsByFilters("SHANKEPHONE:ORDER_INFO",scvf1);
	}
}
