package com.xtransformers.util;

import com.google.common.collect.Sets;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.List;
import java.util.Set;

public final class TenantsUtils {

    private static final PulsarAdmin PULSAR_ADMIN = PulsarAdminFactory.getInstance();

    public static List<String> list() throws PulsarAdminException {
        return PULSAR_ADMIN.tenants().getTenants();
    }

    public static void create(String tenant, String pulsarCluster) throws PulsarAdminException {
        Set<String> allowedClusters = Sets.newHashSet(pulsarCluster);
        TenantInfo config = TenantInfo.builder().allowedClusters(allowedClusters).build();
        PULSAR_ADMIN.tenants().createTenant(tenant, config);
    }

    public static void delete(String tenant) throws PulsarAdminException {
        PULSAR_ADMIN.tenants().deleteTenant(tenant);
    }

}
