package com.xtransformers.service;

import com.google.gson.Gson;
import com.xtransformers.util.Constant;
import com.xtransformers.util.PulsarAdminFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TopicsUtilTest {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    @Test
    public void testNonPartitionedTopic() throws PulsarAdminException {
        String tenant = "java_api_tenant";
        //TenantsUtils.create(tenant, Constant.CLUSTER);

        String namespace = "java_api_namespace";
        String namespaceFullPath = tenant + "/" + namespace;
        //NamespacesUtils.create(namespaceFullPath);

        String topic = "java_api_topic";
        String topicFullPath = Constant.PREFIX_PERSISTENT + namespaceFullPath + "/" + topic;
        TopicsUtil.createNonPartitioned(topicFullPath);

        String topic2 = "java_api_topic2";
        String topicFullPath2 = Constant.PREFIX_NON_PERSISTENT + namespaceFullPath + "/" + topic2;
        TopicsUtil.createNonPartitioned(topicFullPath2);

        List<String> topics = TopicsUtil.list(namespaceFullPath);
        for (String each : topics) {
            LOGGER.info("topic : {}", each);
        }

        PulsarAdminFactory.close();
    }

    @Test
    public void testPartitionedTopic() throws PulsarAdminException {
        String tenant = "java_api_tenant";
        //TenantsUtils.create(tenant, Constant.CLUSTER);

        String namespace = "java_api_namespace";
        String namespaceFullPath = tenant + "/" + namespace;
        //NamespacesUtils.create(namespaceFullPath);

        String topic3 = "java_api_topic3";
        String topicFullPath3 = Constant.PREFIX_PERSISTENT + namespaceFullPath + "/" + topic3;
        TopicsUtil.createPartitioned(topicFullPath3, 5);

        String topic4 = "java_api_topic4";
        String topicFullPath4 = Constant.PREFIX_NON_PERSISTENT + namespaceFullPath + "/" + topic4;
        TopicsUtil.createPartitioned(topicFullPath4, 5);

        List<String> topics = TopicsUtil.listPartitioned(namespaceFullPath);
        for (String each : topics) {
            LOGGER.info("topic : {}", each);
        }

        PulsarAdminFactory.close();
    }

    @Test
    public void test() throws PulsarAdminException {
        String tenant = "java_api_tenant";

        String namespace = "java_api_namespace";
        String namespaceFullPath = tenant + "/" + namespace;

        List<String> topics = TopicsUtil.list(namespaceFullPath);
        for (String each : topics) {
            LOGGER.info("topic : {}", each);
        }
    }

    @Test
    public void testUpdate() throws PulsarAdminException {
        TopicsUtil.updatePartitioned(Constant.TOPIC, 9);

        PartitionedTopicMetadata metadata = TopicsUtil.getMetadata(Constant.TOPIC);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("metadata {}", new Gson().toJson(metadata));
        }
    }

}