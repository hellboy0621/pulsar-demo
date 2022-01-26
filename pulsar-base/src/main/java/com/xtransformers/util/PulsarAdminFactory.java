package com.xtransformers.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

public final class PulsarAdminFactory {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    private static PulsarAdmin pulsarAdmin;

    static {
        try {
            pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(ConfigProvider.SERVER_HTTP_URL).build();
        } catch (PulsarClientException e) {
            LOGGER.error("build PulsarAdmin error ", e);
        }
    }

    public static PulsarAdmin getInstance() {
        return pulsarAdmin;
    }

    public static void close() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

}
