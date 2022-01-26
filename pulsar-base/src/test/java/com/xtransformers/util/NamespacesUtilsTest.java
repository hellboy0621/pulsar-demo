package com.xtransformers.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.Test;

import java.util.List;

public class NamespacesUtilsTest {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    @Test
    public void testLog() {
        LOGGER.info("a");
        LOGGER.error("b", new RuntimeException("cde"));
        LOGGER.warn("hello");
    }

    @Test
    public void test() throws PulsarAdminException {

        String tenant = "java_api_tenant";
        TenantsUtils.create(tenant, Constant.CLUSTER);

        String namespace = "java_api_namespace";
        String namespaceFullPath = tenant + "/" + namespace;
        NamespacesUtils.create(namespaceFullPath);

        List<String> namespaces = NamespacesUtils.list(tenant);
        for (String each : namespaces) {
            LOGGER.info("namespace : " + each);
        }

        NamespacesUtils.delete(namespaceFullPath);

        LOGGER.info("after delete namespace.");

        namespaces = NamespacesUtils.list(tenant);
        for (String each : namespaces) {
            LOGGER.info("namespace : " + each);
        }

        PulsarAdminFactory.close();

    }

}