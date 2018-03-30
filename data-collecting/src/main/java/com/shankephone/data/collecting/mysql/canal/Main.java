package com.shankephone.data.collecting.mysql.canal;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.shankephone.data.common.kafka.KafkaHelper;
import com.shankephone.data.common.util.PropertyAccessor;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String args[]) throws Throwable {
    	//启动canal server
    	CanalLauncher.execute();
    	logger.info("## start the canal client.");
    	ClientIdentity clientIdentity = new ClientIdentity();
    	clientIdentity.setDestination(PropertyAccessor.getProperty("canal.destination"));
    	clientIdentity.setClientId(Short.valueOf(PropertyAccessor.getProperty("canal.clientId")));
    	Producer<String, String> producer = KafkaHelper.createProducer();
    	CanalClient canalClient = new CanalClient(clientIdentity, producer);
    	canalClient.start();
    	logger.info("## the canal client is running now ......");
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    canalClient.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }
        });
    }
    
}
