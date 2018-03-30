package com.shankephone.data.common.web.socket;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.redisson.api.RPatternTopic;
import org.redisson.api.RTopic;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.ClassUtils;
/**
 * 浏览器WebSocket订阅相关保持者
 * 
 * @author DuXiaohua
 * @version 2017年8月29日 下午4:08:55
 */
public class SubHolder {
	
	//每个session对应一个SubHolder
	private static Map<WebSocketSession, SubHolder> subHolderMap = new ConcurrentHashMap<>();
	//数据初始化时，topic和处理类的对应关系
	private static Map<String, String> subDataInitMap = new HashMap<>();
	//当前session的所有订阅信息
	private Set<SubInfo> subInfoSet = ConcurrentHashMap.newKeySet();
    //当前session
	private WebSocketSession session;
	//当前session对应的redis topic listener，所有当前session的订阅共用一个listener
	private SubMessageListener listener;
	
	static {
		//加载数据初始化时，topic和处理类的对应关系
		try (InputStream is = ClassUtils.getResourceAsStream("subDataInit.xml")) {
			SAXReader reader = new SAXReader();
			Document doc = reader.read(is);
			Element rootEl = doc.getRootElement();
			List<Element>  els = rootEl.elements();
			for (Element el : els) {
				String topic = el.attributeValue("topic");
				String clazz = el.attributeValue("class");
				subDataInitMap.put(topic, clazz);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * 获取当前session对应的SubHolder
	 * @author DuXiaohua
	 * @date 2017年9月20日 上午12:40:46
	 * @param session
	 * @return
	 */
	public static SubHolder getInstance(WebSocketSession session) {
		SubHolder subHolder = subHolderMap.get(session);
		if (subHolder == null) {
			subHolder = new SubHolder(session);
			SubHolder oldSubHolder = subHolderMap.putIfAbsent(session, subHolder);
			if (oldSubHolder != null) {
				subHolder = oldSubHolder;
			}
		}
		return subHolder;
	}
	
	public static void removeInstance(WebSocketSession session) {
		subHolderMap.remove(session);
	}

	public SubHolder(WebSocketSession session) {
		this.session = session;
		this.listener = new SubMessageListener(this.session);
	}
	
	/**
	 * 订阅redis topic，及推送初始化数据
	 * @author DuXiaohua
	 * @date 2017年9月20日 上午12:44:14
	 * @param subInfo
	 */
	public void sub(SubInfo subInfo) {
		boolean absent = subInfoSet.add(subInfo);
		if (absent) {
			if (subInfo.isPattern()) {
				RPatternTopic<String> patternTopic = RedisUtils.getRedissonClient().getPatternTopic(subInfo.getTopic());
				patternTopic.addListener(listener);
			} else {
				RTopic<String> topic = RedisUtils.getRedissonClient().getTopic(subInfo.getTopic());
				topic.addListener(listener);
			}
			initSub(subInfo);
		}
	}
	/**
	 * 推送初始化数据
	 * @author DuXiaohua
	 * @date 2017年9月20日 上午12:44:51
	 * @param subInfo
	 */
	public void initSub(SubInfo subInfo) {
		String initClass = null;
		for (String topicRegex : subDataInitMap.keySet()) {
			Pattern pattern = Pattern.compile(topicRegex);
			Matcher matcher = pattern.matcher(subInfo.getTopic());
			if (matcher.find()) {
				initClass = subDataInitMap.get(topicRegex);
				break;
			}
		}
		if (StringUtils.isBlank(initClass)) {
			return;
		}
		try {
			SubDataInitProcessor processor = (SubDataInitProcessor) Class.forName(initClass).newInstance();
			Object obj = processor.process(subInfo);
			if (obj == null) {
				return;
			}
			SubMessage subMessage = new SubMessage();
			subMessage.setTopic(subInfo.getTopic());
			subMessage.setPattern(subInfo.isPattern());
			subMessage.setInit(true);
			subMessage.setData(obj);
			synchronized (session) {
				session.sendMessage(new TextMessage(JSONObject.toJSONString(subMessage)));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 取消所有订阅
	 * @author DuXiaohua
	 * @date 2017年9月20日 上午12:45:37
	 */
	public void unsub() {
		for (SubInfo subInfo : subInfoSet) {
			if (subInfo.isPattern()) {
				RPatternTopic<String> patternTopic = RedisUtils.getRedissonClient().getPatternTopic(subInfo.getTopic());
				patternTopic.removeListener(listener);
			} else {
				RTopic<String> topic = RedisUtils.getRedissonClient().getTopic(subInfo.getTopic());
				topic.removeListener(listener);
			}
		}
		removeInstance(session);
	}
	
}
