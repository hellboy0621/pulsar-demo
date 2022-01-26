package com.xtransformers.service;


import com.xtransformers.service.TenantsUtils;
import com.xtransformers.util.Constant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.Test;

import java.util.List;

public class TenantsUtilsTest {

    private static final Logger LOGGER = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    @Test
    public void test() throws PulsarAdminException {
        List<String> tenants = TenantsUtils.list();
        for (String each : tenants) {
            LOGGER.info("tenant: {}", each);
        }

        TenantsUtils.create("java_api_tenant1", Constant.CLUSTER);

        LOGGER.info("after create tenant.");

        tenants = TenantsUtils.list();
        for (String each : tenants) {
            LOGGER.info("tenant: {}", each);
        }
    }

    @Test
    public void testDeleteTenant() throws PulsarAdminException {

        List<String> tenants = TenantsUtils.list();
        for (String each : tenants) {
            LOGGER.info("tenant: {}", each);
        }

        String tenant = "java_api_tenant";
        TenantsUtils.delete(tenant);
        tenant = "java_api_tenant1";
        TenantsUtils.delete(tenant);

        LOGGER.info("after delete tenant.");

        tenants = TenantsUtils.list();
        for (String each : tenants) {
            LOGGER.info("tenant: {}", each);
        }
    }

}