package com.xtransformers.service.consumer;

import com.xtransformers.util.PulsarClientFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConsumerUtilsTest {

    @Test
    public void test() throws PulsarClientException {
        ConsumerUtils.consume();
        PulsarClientFactory.close();
    }

    @Test
    public void testConsumeSchema() throws PulsarClientException {
        ConsumerUtils.consumeSchema();
        PulsarClientFactory.close();
    }

}