package com.xtransformers.service;

import com.xtransformers.util.PulsarAdminFactory;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.List;

public class TopicsUtil {

    private static final PulsarAdmin PULSAR_ADMIN = PulsarAdminFactory.getInstance();

    public static void createNonPartitioned(String topic) throws PulsarAdminException {
        PULSAR_ADMIN.topics().createNonPartitionedTopic(topic);
    }

    public static List<String> list(String namespace) throws PulsarAdminException {
        return PULSAR_ADMIN.topics().getList(namespace);
    }

    public static void createPartitioned(String topic, int numPartitions) throws PulsarAdminException {
        PULSAR_ADMIN.topics().createPartitionedTopic(topic, numPartitions);
    }

    public static List<String> listPartitioned(String namespace) throws PulsarAdminException {
        return PULSAR_ADMIN.topics().getPartitionedTopicList(namespace);
    }

    public static void updatePartitioned(String topic, int numPartitions) throws PulsarAdminException {
        PULSAR_ADMIN.topics().updatePartitionedTopic(topic, numPartitions);
    }

    public static PartitionedTopicMetadata getMetadata(String topic) throws PulsarAdminException {
        return PULSAR_ADMIN.topics().getPartitionedTopicMetadata(topic);
    }

    public static void deletePartitioned(String topic) throws PulsarAdminException {
        PULSAR_ADMIN.topics().deletePartitionedTopic(topic);
    }

}
