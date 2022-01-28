package com.xtransformers.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public final class PulsarClientFactory {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    private static PulsarClient pulsarClient;

    static {
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(ConfigProvider.CLIENT_SERVICE_URL)
                    .build();
        } catch (PulsarClientException e) {
            LOGGER.error("init pulsar client error ", e);
        }
    }

    public static PulsarClient getInstance() {
        return pulsarClient;
    }

    public static void close() throws PulsarClientException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }

}
