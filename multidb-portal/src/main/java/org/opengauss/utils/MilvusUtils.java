/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.utility.response.CheckHealthResp;

import java.util.Locale;

/**
 * MilvusUtils
 *
 * @since 2025/10/30
 */
public class MilvusUtils {
    /**
     * Create a Milvus client V2 without authentication.
     *
     * @param ip the Milvus ip
     * @param port the Milvus port
     * @return the Milvus client V2
     */
    public static MilvusClientV2 createMilvusClientV2(String ip, int port) {
        return createMilvusClientV2(ip, port, null, null, null);
    }

    /**
     * Create a Milvus client V2.
     *
     * @param ip the Milvus ip
     * @param port the Milvus port
     * @param database the Milvus database name (optional)
     * @param username the Milvus username (optional)
     * @param password the Milvus password (optional)
     * @return the Milvus client V2
     */
    public static MilvusClientV2 createMilvusClientV2(
            String ip, int port, String database, String username, String password
    ) {
        if (ip == null || ip.isEmpty()) {
            throw new IllegalArgumentException("IP cannot be null or empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Port must be a positive integer");
        }

        String uri = String.format(Locale.ROOT, "http://%s:%d", ip, port);
        ConnectConfig connectConfig = ConnectConfig.builder().uri(uri).build();

        if (database != null && !database.isEmpty()) {
            connectConfig.setDbName(database);
        }
        if (username != null && password != null) {
            connectConfig.setUsername(username);
            connectConfig.setPassword(password);
        }

        return new MilvusClientV2(connectConfig);
    }

    /**
     * Check if the Milvus client V2 is healthy.
     *
     * @param milvusClientV2 the Milvus client V2
     * @return true if the Milvus client V2 is healthy, false otherwise
     */
    public static boolean isHealthy(MilvusClientV2 milvusClientV2) {
        if (milvusClientV2 == null) {
            throw new IllegalArgumentException("MilvusClientV2 cannot be null");
        }

        CheckHealthResp checkHealthResp = milvusClientV2.checkHealth();
        return checkHealthResp.getIsHealthy();
    }

    /**
     * Get the version number of the Milvus cluster.
     *
     * @param milvusClientV2 the Milvus client V2
     * @return the version number of the Milvus cluster
     */
    public static String getMilvusVersion(MilvusClientV2 milvusClientV2) {
        if (milvusClientV2 == null) {
            throw new IllegalArgumentException("MilvusClientV2 cannot be null");
        }

        return milvusClientV2.getServerVersion();
    }

    /**
     * Close the Milvus client V2.
     *
     * @param milvusClientV2 the Milvus client V2
     */
    public static void closeMilvusClientV2(MilvusClientV2 milvusClientV2) {
        if (milvusClientV2 == null) {
            return;
        }

        milvusClientV2.close();
    }
}
