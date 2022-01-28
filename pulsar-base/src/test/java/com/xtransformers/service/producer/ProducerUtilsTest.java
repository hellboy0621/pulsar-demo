package com.xtransformers.service.producer;

import com.xtransformers.domain.Equipment;
import com.xtransformers.util.PulsarClientFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

import java.util.UUID;

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

    /**
     * 2022-01-28 09:53:08.691 [main] INFO  [] [] [event] .sendMessageSchema:81 - equipment
     * Equipment{id='342090db2aef4992910c943803c86b9a', name='JR100-34861', sn='38ebd9441c344fe9bd08e9d7375b6908', type='JR100'}
     *
     * ----- got message -----
     * key:[null], properties:[], content:@342090db2aef4992910c943803c86b9aJR100-34861@38ebd9441c344fe9bd08e9d7375b6908
     * JR100
     * @throws PulsarClientException
     */
    @Test
    public void testSchema() throws PulsarClientException {
        Equipment equipment = new Equipment();
        equipment.setId(UUID.randomUUID().toString().replace("-", ""));
        equipment.setName("JR100-34861");
        equipment.setSn(UUID.randomUUID().toString().replace("-", ""));
        equipment.setType("JR100");
        ProducerUtils.sendMessageSchema(equipment);
        PulsarClientFactory.close();
    }

}