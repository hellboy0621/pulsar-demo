package com.xtransformers.service.producer;

import com.xtransformers.util.PulsarClientFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

public class ProducerUtilsTest {

    /**
     * 2022-01-27T06:25:05,634+0000 [pulsar-client-io-1-1] INFO  com.scurrilous.circe.checksum.Crc32cIntChecksum - SSE4.2 CRC32C provider initialized
     * ----- got message -----
     * key:[null], properties:[], content:Hello pulsar api world.
     */
    @Test
    public void testSendMessage() throws PulsarClientException {
        ProducerUtils.sendMessage("Hello pulsar api world.");
        PulsarClientFactory.close();
    }

    /**
     * ----- got message -----
     * key:[null], properties:[], content:Hello pulsar api world async.
     */
    @Test
    public void testSendMessageAsync() throws PulsarClientException, InterruptedException {
        ProducerUtils.sendMessageAsync("Hello pulsar api world async.");
        PulsarClientFactory.close();
    }

}