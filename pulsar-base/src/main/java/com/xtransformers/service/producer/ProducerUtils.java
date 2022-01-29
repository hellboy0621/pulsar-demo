package com.xtransformers.service.producer;

import com.xtransformers.domain.Equipment;
import com.xtransformers.util.Constant;
import com.xtransformers.util.PulsarClientFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import java.util.concurrent.TimeUnit;

public class ProducerUtils {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    /**
     * 1. 创建 Pulsar 客户端对象
     * 2. 通过客户端创建生产者对象
     * 3. 使用生产者发送数据
     * 4. 释放资源
     */

    /**
     * 同步方式发送消息
     *
     * @param message 消息
     * @throws PulsarClientException if the producer creation fails
     */
    public static void sendMessage(String message) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(Constant.TOPIC)
                .create();
        producer.send(message);
        producer.close();
    }

    /**
     * 异步发送方案
     * 会先将数据写入到客户端缓存中，当缓存中数据达到一批后，才会进行发送操作
     * 1. 不关闭 producer 和 pulsarClient
     * 2. 发送完成后，让程序等一下，让其讲缓冲区中数据刷新到 pulsar 上，然后再结束
     *
     * @param message 消息
     * @throws PulsarClientException if the producer creation fails
     */
    public static void sendMessageAsync(String message) throws PulsarClientException, InterruptedException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(Constant.TOPIC)
                .create();
        producer.sendAsync(message);
        TimeUnit.SECONDS.sleep(1);
        producer.close();
    }

    /**
     * Schema 方式
     * org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException:
     * {
     *    "errorMsg": "org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException:
     *      Incompatible schema: exists schema type STRING, new schema type AVRO caused by org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException:
     *        Incompatible schema: exists schema type STRING, new schema type AVRO",
     *    "reqId": 4127593216745531395,
     *    "remote": "node3/172.26.20.252:6650",
     *    "local": "/172.26.20.116:60215"
     * }
     */
    public static void sendMessageSchema(Equipment equipment) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();

        AvroSchema<Equipment> schema = AvroSchema.of(Equipment.class);
        Producer<Equipment> producer = pulsarClient.newProducer(schema)
                .topic(Constant.TOPIC + "1")
                .create();

        LOGGER.info("equipment {}", equipment);
        producer.send(equipment);

        producer.close();
    }

    public static void sendMessageBatch(String message) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.getInstance();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(Constant.TOPIC)
                .create();
        for (int i = 0; i < 200; i++) {
            producer.send(message + i);
        }
        producer.close();
    }
}
