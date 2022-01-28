package com.xtransformers.service.consumer;

import com.xtransformers.util.Constant;
import com.xtransformers.util.PulsarClientFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;

public class ConsumerUtils {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    /**
     * 1. 创建 PulsarClient 对象
     * 2. 基于客户端构建消费者对象
     * 3. 循环从消费者中读取数据
     * 4. 释放资源
     */

    public static void consume() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(Constant.TOPIC)
                .subscriptionName("sub_03")
                .subscribe();
        while (true) {
            Message<String> message = consumer.receive();
            try {
                String msg = message.getValue();
                LOGGER.info("receive message : {}", msg);
                consumer.acknowledge(message);
            } catch (Throwable t) {
                LOGGER.warn("Failed to process message");
                consumer.negativeAcknowledge(message);
            }
        }
    }

}
