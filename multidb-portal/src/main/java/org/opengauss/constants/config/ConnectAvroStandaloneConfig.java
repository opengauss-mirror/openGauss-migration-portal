/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * Connect Avro Standalone Config
 *
 * @since 2025/5/7
 */
public class ConnectAvroStandaloneConfig {
    /**
     * key converter schema registry url
     */
    public static final String SCHEMA_REGISTRY_URL_FOR_KEY_CONVERTER = "key.converter.schema.registry.url";

    /**
     * rest port
     */
    public static final String REST_PORT = "rest.port";

    /**
     * plugin path
     */
    public static final String PLUGIN_PATH = "plugin.path";

    /**
     * offset storage file filename
     */
    public static final String OFFSET_STORAGE_FILE_FILENAME = "offset.storage.file.filename";

    /**
     * connector client config override policy
     */
    public static final String CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY = "connector.client.config.override.policy";

    /**
     * bootstrap servers
     */
    public static final String KAFKA_SERVERS = "bootstrap.servers";

    /**
     * value converter schema registry url
     */
    public static final String SCHEMA_REGISTRY_URL_FOR_VALUE_CONVERTER = "value.converter.schema.registry.url";

    private ConnectAvroStandaloneConfig() {
    }
}
