package com.xtransformers.service.consumer;

import com.xtransformers.domain.Equipment;
import com.xtransformers.util.Constant;
import com.xtransformers.util.PulsarClientFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import java.util.concurrent.TimeUnit;

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
                LOGGER.warn("Failed to process message", t);
                consumer.negativeAcknowledge(message);
            }
        }
    }

    public static void consumeSchema() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        Consumer<Equipment> consumer = pulsarClient.newConsumer(AvroSchema.of(Equipment.class))
                .topic(Constant.TOPIC + "1")
                .subscriptionName("sub_04")
                .subscribe();
        while (true) {
            Message<Equipment> message = consumer.receive();
            try {
                Equipment msg = message.getValue();
                LOGGER.info("received msg : {}", msg);
                consumer.acknowledge(message);
            } catch (Exception e) {
                LOGGER.error("Failed to process message", e);
                consumer.negativeAcknowledge(message);
            }
        }
    }

    public static void consumeBatch() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        // 设置批量读取参数配置
        BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .maxNumBytes(1024 * 1024)
                .maxNumMessages(100)
                .timeout(2000, TimeUnit.MILLISECONDS)
                .build();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(Constant.TOPIC)
                .subscriptionName("sub_04")
                .batchReceivePolicy(batchReceivePolicy)
                .subscribe();

        while (true) {
            Messages<String> messages = consumer.batchReceive();
            for (Message<String> message : messages) {
                try {
                    String msg = message.getValue();
                    LOGGER.info("receive message : {}", msg);
                    consumer.acknowledge(message);
                } catch (Exception e) {
                    LOGGER.error("consume message error ", e);
                    consumer.negativeAcknowledge(message);
                }
            }
        }
    }

}
