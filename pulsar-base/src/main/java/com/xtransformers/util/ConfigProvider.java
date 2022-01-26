package com.xtransformers.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class ConfigProvider {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();
        InputStream in = ConfigProvider.class.getClassLoader().getResourceAsStream("application.properties");
        assert in != null;
        InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
        try {
            PROPERTIES.load(isr);
        } catch (IOException e) {
            LOGGER.error("init properties error ", e);
        }
    }

    public static final String SERVER_HTTP_URL = PROPERTIES.getProperty("pulsar.admin.service-http-url", "http://node1:8080,node2:8080,node3:8080");

}
