package com.shankephone.data.visualization.computing.common.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Constants implements Serializable {
	
	private static final long serialVersionUID = 6105709565812483381L;
	
	/**
	 * 先享后付交易类型：53.进站，54.出站
	 */
	public static final String XXHF_TYPE_ENTRY = "53";
	public static final String XXHF_TYPE_EXIT = "54";
	
	/*订单类型*/
	public static Map<String,String> orderTypeMap = new HashMap<String,String>();
	public static final String ORDER_TYPE_DCP = "dcp";  		//单程票
	public static final String ORDER_TYPE_XXHF = "xxhf";		//先享后付
	public static final String ORDER_TYPE_XFHX = "xfhx";		//先付后享
	public static final String ORDER_TYPE_NFC = "nfc";		//长沙NFC
	public static final String ORDER_TYPE_COFFEE = "coffee";	//蜂格咖啡
	public static final String ORDER_TYPE_USER = "user";	//用户信息
	static {
		orderTypeMap.put("dcp","单程票");
		orderTypeMap.put("xxhf","先享后付");
		orderTypeMap.put("xfhx","先付后享");
		orderTypeMap.put("nfc","长沙NFC");
		orderTypeMap.put("coffee","蜂格咖啡");
	}
	
	public static Map<String,String> coffeeShopMaps = new HashMap<String,String>();
	static {
		coffeeShopMaps.put("510000002_2009","南洲店");
		coffeeShopMaps.put("510000002_2005","赤岗店");
		coffeeShopMaps.put("510000002_2003","三元里店");
		coffeeShopMaps.put("510000002_2008","公园前店");
		coffeeShopMaps.put("510000002_2006","晓港店");
		coffeeShopMaps.put("510000002_2007","芳村店");
		coffeeShopMaps.put("510000002_2002","体育西路店");
		coffeeShopMaps.put("510000002_2004","琶洲店");
	}
	
	public static final String PAY_TYPE_WX = "wx";
	public static final String PAY_TYPE_SKF = "skf";
	public static final String PAY_TYPE_ZFB = "zfb";
	public static final String PAY_TYPE_YGPJ = "ygpj";
	public static final String PAY_TYPE_GZDT = "gzdt";
	public static final String PAY_TYPE_HB = "hb";
	public static final String PAY_TYPE_QT = "qt";
	
	/*来源区分*/
	public static final String SKF_PAY_TYPE="-1,0,1,3,5,8,11";
	public static final String WX_PAY_TYPE="12,13,14,7";
	public static final String ZFB_PAY_TYPE="6";
	public static final String YGPJ_PAY_TYPE="2,4";
	public static final String HB_PAY_TYPE="9";
	public static final String gzdt_PAY_TYPE="10";
	
	public static Map<String,String> sourceMap_New = new HashMap<String,String>();
	static {
		sourceMap_New.put("skf","闪客蜂APP");
		sourceMap_New.put("wx","微信小程序");
		sourceMap_New.put("zfb","支付宝城市服务");
		sourceMap_New.put("ygpj","云购票机");
		sourceMap_New.put("gzdt","广州地铁");
		sourceMap_New.put("hb","和包app");
		sourceMap_New.put("qt","其它");
	}
	
	public static Map<String,String> sourceMap = new HashMap<String,String>();
	static {
		sourceMap.put("1","购票机（TVIP设备）");
		sourceMap.put("2","闪客蜂APP");
		sourceMap.put("3","闪客蜂公众号");
		sourceMap.put("4","地铁公众号");
		sourceMap.put("5","支付宝城市服务或支付宝服务窗");
		sourceMap.put("99","其它");
	}
	
	public static Map<String,String> marchantMap = new HashMap<String,String>();
	static {
		sourceMap.put("510000001","4401");
		sourceMap.put("450000001","4500");
		sourceMap.put("530000001","5300");
		sourceMap.put("300000001","1200");
		sourceMap.put("410000001","4100");
	}
	
	public static Map<String,String> cityMap = new HashMap<String,String>();
	static {
		cityMap.put("1200", "天津");
		cityMap.put("2660", "青岛");
		cityMap.put("4100", "长沙");
		cityMap.put("4401", "广州");
		cityMap.put("4500", "郑州");
		cityMap.put("5300", "南宁");
		cityMap.put("7100", "西安");
		cityMap.put("5190", "珠海");
		cityMap.put("0000", "全国");
	}
	
	/**
	 * 统计时段类型：0-小时，1-天，2-周，3-月，4-年
	 */
	public final static Integer PERIOD_TYPE_HOUR = 0;
	public final static Integer PERIOD_TYPE_DATE = 1;
	public final static Integer PERIOD_TYPE_WEEK = 2;
	public final static Integer PERIOD_TYPE_MONTH = 3;
	public final static Integer PERIOD_TYPE_YEAR = 4;
	
	/**
	 * 维度分隔符
	 */
	public final static String DIMENSION_SEPERATOR = "_";
	
	/**
	 * 用户数量统计：一级分类：2000，二级分类：新用户-2100，老用户-2200
	 */
	public final static String ANALYSE_USER_NUM_NEW = "2100";
	public final static String ANALYSE_USER_NUM_OLD = "2200";
	
	public final static String REDIS_NAMESPACE_TICKET = "ticket:";
	public final static String REDIS_NAMESPACE_TICKET_PAYMENT = "pay:type:";
	public final static String REDIS_NAMESPACE_USER = "user:";
	public final static String REDIS_NAMESPACE_USER_ACTIVE = "user:active";
	
	/**
	 * 支付类型：
	 * 支付宝、微信、中移动、翼支付、首信翼支付、银联、其它
	 */
	public final static String PAYMENT_TYPE_ZIFB = "zfb";
	public final static String PAYMENT_TYPE_WEIX = "wx";
	public final static String PAYMENT_TYPE_ZHONGYD = "zyd";
	public final static String PAYMENT_TYPE_YIZF = "yzf";
	public final static String PAYMENT_TYPE_SHOUXYZF = "sxyzf";
	public final static String PAYMENT_TYPE_YINL = "yl";
	public final static String PAYMENT_TYPE_OTHER = "other";
	
	/**
	 * 支付渠道，代码名称映射
	 */
	public static Map<String,String> paymentMaps = new HashMap<String,String>();
	static {
		paymentMaps.put("zfb", "支付宝");
		paymentMaps.put("wx", "微信");
		paymentMaps.put("zyd", "中移动");
		paymentMaps.put("yzf", "翼支付");
		paymentMaps.put("sxyzf", "首信易支付");
		paymentMaps.put("yl", "银联");
		paymentMaps.put("other", "其它");
	}
	
	/**
	 * 实时交易记录topic
	 */
	public static final String REDIS_TOPIC_TICKET_RECORD = "ticket:record:";
	public static final String REDIS_TOPIC_LAST_TIME = "ticket:lastTime";
	
	/**
	 * 用户来源的redis topic名称或缓存名称
	 */
	public static final String REDIS_USER_SOURCE_TOPIC = "user:source:vol:";
	/**
	 * redis中用户ID的set名称
	 */
	public static final String REDIS_USER_ID_SET = "user:uniq:";
	/**
	 * 全国的代码
	 */
	public static final String NATION_CODE = "0000"; 
	
	/**
	 * 云闸机过闸统计Redis Key Topic
	 */
	public static final String GATETRANSIT_REDIS_KEY = "gate:vol:4401";
	
	/**
	 * DATA_YPT_TRAN表中为云闸机过闸的TRAN_TYPE
	 */
	public static final List<String> DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT = new ArrayList<>();
	
	static {
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("53");//实体卡进站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("54");//实体卡出站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("62");//APM线出站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("63");//APM线进站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("73");//二维码票进站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("74");//二维码票出站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("90");//金融IC卡进站
		DATA_YPT_TRAN_TRAN_TYPE_GATETRANSIT.add("91");//金融IC卡出站
	}
	
	/**
	 * 云闸机过闸车票类型
	 */
	public static String getGateTransitTypeName(String type) {
		switch (type) {
			case "01" : return "dcp";//单程票
			case "02" : return "czp";//储值票
			case "03" : return "ccp";//乘次票
			case "05" : return "jnp";//纪念票
			case "07" : return "ygp";//员工票
			case "08" : return "qrcode";//二维码
			case "09" : return "yct";//羊城通
			case "10" : return "ic";//金融IC卡
			case "11" : return "cloud";//地铁云卡
			default : return "other";//其他
		}
	}
	
	//进站
	public static final String XXHF_TRX_TYPE_ENTRY = "53";
	//出站
	public static final String XXHF_TRX_TYPE_EXIT = "54";
	//补票
	public static final String XXHF_TRX_TYPE_UNUSUAL = "56";
	
}
