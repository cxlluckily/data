package com.shankephone.data.collecting.mysql.canal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.shankephone.data.collecting.config.Config;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.util.XmlUtils;

public class CanalClient {

	private final static Logger             logger             = LoggerFactory.getLogger(CanalClient.class);
    private static final int                maxEmptyTimes      = 10;
    private volatile boolean                running            = false;
    private Thread.UncaughtExceptionHandler handler            = new Thread.UncaughtExceptionHandler() {

                                                                     public void uncaughtException(Thread t, Throwable e) {
                                                                         logger.error("parse events has an error", e);
                                                                     }
                                                                 };
    private Thread                          thread             = null;
    private CanalServerWithEmbedded         embededCanalServer = CanalServerWithEmbedded.instance();
    private ClientIdentity                  clientIdentity;
    private Producer<String, String> producer;
    //Kafka Callback异常
    private Exception exception;
    private Config config = XmlUtils.xml2Java("collectingConfig.xml", Config.class);
    
    public CanalClient(ClientIdentity clientIdentity, Producer<String, String> producer){
        this.clientIdentity = clientIdentity;
        this.producer = producer;
    }
    
    public void start() {
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        int batchSize = 5 * 1024;
        clientIdentity.setFilter(config.getFilterRegex());
        embededCanalServer.subscribe(clientIdentity);
        int emptyTimes = 0;
        while (running) {
            try {
                Message message = embededCanalServer.getWithoutAck(clientIdentity, batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                	emptyTimes = emptyTimes + 1;
                	applyWait(emptyTimes);
                } else {
                	emptyTimes = 0;
                	try {
						buildEventData(message);
						producer.flush();
						if (exception != null) {
							throw exception;
						}
						embededCanalServer.ack(clientIdentity, batchId);
					} catch (Exception e) {
						logger.error("写入kafka失败", e);
						exception = null;
						embededCanalServer.rollback(clientIdentity, batchId);
					}
                }
            } catch (Exception e) {
            	e.printStackTrace();
                logger.error("process error!", e);
            }
        }
    }

    private void applyWait(int emptyTimes) {
        int newEmptyTimes = emptyTimes > maxEmptyTimes ? maxEmptyTimes : emptyTimes;
        if (emptyTimes <= 3) { // 3次以内
            Thread.yield();
        } else { // 超过3次，最多只sleep 10ms
            LockSupport.parkNanos(1000 * 1000L * newEmptyTimes);
        }
    }
    
    private void buildEventData(Message message) {
    	List<Entry> entries = message.getEntries();
    	for (Entry entry : entries) {
    		if (entry.getEntryType() != EntryType.ROWDATA) {
    			 continue;
    		}
    		buildEventData(entry);
    	}
    }

    private void buildEventData(Entry entry) {
        RowChange rowChange = null;
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("parser of canal-event has an error , data:" + entry.toString(), e);
        }
        if (rowChange == null) {
            return;
        }
        EventType eventType = EventType.valueOf(rowChange.getEventType().name());
        if (eventType.isDml()) {
			for (RowData rowData : rowChange.getRowDatasList()) {
				buildEventData(entry, rowChange, rowData, eventType);
			}
        } else if (eventType.isDdl()) {
        	buildDdlEvent(entry, rowChange, eventType);
        }
    }
    
    private void buildDdlEvent(Entry entry, RowChange rowChange, EventType eventType) {
    	String schemaName = entry.getHeader().getSchemaName();
    	String tableName = entry.getHeader().getTableName();
    	EventData eventData = new EventData();
        eventData.setServerId(entry.getHeader().getServerId());
        eventData.setSchemaName(schemaName);
        eventData.setTableName(tableName);
        eventData.setExecuteTime(entry.getHeader().getExecuteTime());
        eventData.setLogfile(entry.getHeader().getLogfileName());
        eventData.setLogfileOffset(entry.getHeader().getLogfileOffset());
        eventData.setEventTypeName(rowChange.getEventType().name().toLowerCase());
        eventData.setSql(rowChange.getSql());
        
        String kafkaKey = tableName;//DDL操作已表名作为kafka key
        String kafkaValue = JSONObject.toJSON(eventData).toString();
        
        if (logger.isDebugEnabled()) {
            logger.debug("DDL【topic】" + config.getKafkaTopic(schemaName, tableName) + "【key】" + kafkaKey);
            logger.debug("【value】"+kafkaValue);
        }
        sendToKafka(config.getKafkaTopic(schemaName, tableName), kafkaKey, kafkaValue);
    }
    
    /**
     * 解析出从canal中获取的Event事件<br>
     * Mysql:可以得到所有变更前和变更后的数据.<br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在beforeColumns中存放变更前的所有数据,在afterColumns中存放变更后的所有数据<br>
     */
    private void buildEventData(Entry entry, RowChange rowChange, RowData rowData, EventType eventType) {
    	Map<String, Object> keys = new HashMap<String, Object>();
        Map<String, Object> columns = new HashMap<String, Object>();
        List<Column> eventColumns = null;
        if (eventType.isInsert()) {
        	eventColumns = rowData.getAfterColumnsList();
        } else if (eventType.isDelete()) {
        	eventColumns = rowData.getBeforeColumnsList();
        } else if (eventType.isUpdate()) {
        	eventColumns = rowData.getAfterColumnsList();
        } else {
        	return;
        }
        for (Column column : eventColumns) {
            if (column.getIsKey()) {
            	keys.put(column.getName().toUpperCase(), column.getValue());
            }
            columns.put(column.getName().toUpperCase(), column.getValue());
        }
        EventData eventData = new EventData();
        String schemaName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        eventData.setServerId(entry.getHeader().getServerId());
        eventData.setSchemaName(schemaName.toUpperCase());
        eventData.setTableName(tableName.toUpperCase());
        eventData.setExecuteTime(entry.getHeader().getExecuteTime());
        eventData.setLogfile(entry.getHeader().getLogfileName());
        eventData.setLogfileOffset(entry.getHeader().getLogfileOffset());
        eventData.setEventTypeName(rowChange.getEventType().name().toLowerCase());
        eventData.setKeys(keys);
        eventData.setColumns(columns);

        List<String> hashColumns = config.getTableHashColumn(schemaName, tableName);
        String hashValue = ""; 
        if (hashColumns.size() == 0) {			//未配置 取主键值 多字段以逗号分隔
        	for (Object value : keys.values()) {
        		hashValue += "," + value;
        	}
        } else {
        	for (String column : hashColumns) {		//取collectingConfig.xml配置字段值 多字段以逗号分隔
        		hashValue += "," + columns.get(column.toUpperCase());
        	}
        }
        
        String kafkaKey = hashValue.substring(1, hashValue.length());
        String kafkaValue = JSONObject.toJSON(eventData).toString();
        
        if (logger.isDebugEnabled()) {
            logger.debug("【topic】" + config.getKafkaTopic(schemaName, tableName) + "【key】" + kafkaKey);
            logger.debug("【value】" + kafkaValue);
        }
        
        sendToKafka(config.getKafkaTopic(schemaName, tableName), kafkaKey, kafkaValue);
    }
    
    private void sendToKafka(String kafkaTopic, String kafkaKey, String kafkaValue) {
    	producer.send(new ProducerRecord<String, String>(kafkaTopic, kafkaKey, kafkaValue),
       		 (metadata, e) -> {
       			 if (e != null) {
       				 exception = e;
       				 throw new RuntimeException(e);
       			 }
       		 });
    }
}
