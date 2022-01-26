package com.xtransformers.util;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.List;

public final class NamespacesUtils {

    private static final PulsarAdmin PULSAR_ADMIN = PulsarAdminFactory.getInstance();

    public static List<String> list(String tenant) throws PulsarAdminException {
        return PULSAR_ADMIN.namespaces().getNamespaces(tenant);
    }

    public static void create(String namespace) throws PulsarAdminException {
        PULSAR_ADMIN.namespaces().createNamespace(namespace);
    }

    public static void delete(String namespace) throws PulsarAdminException {
        PULSAR_ADMIN.namespaces().deleteNamespace(namespace);
    }

}
