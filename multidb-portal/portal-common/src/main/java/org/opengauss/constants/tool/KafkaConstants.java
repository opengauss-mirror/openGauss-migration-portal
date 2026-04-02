/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * kafka constants
 *
 * @since 2025/4/19
 */
public class KafkaConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "Kafka";

    /**
     * install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "confluent";

    /**
     * install pkg name
     */
    public static final String INSTALL_PKG_NAME = "confluent-community-5.5.1-2.12.zip";

    /**
     * install dir name
     */
    public static final String INSTALL_DIR_NAME = "confluent";

    /**
     * confluent dir name
     */
    public static final String CONFLUENT_DIR_NAME = "confluent-5.5.1";

    /**
     * kafka tmp dir name
     */
    public static final String KAFKA_TMP_DIR_NAME = "kafka-logs";

    /**
     * kafka starter relative path
     */
    public static final String KAFKA_STARTER_RELATIVE_PATH = "bin/kafka-server-start";

    /**
     * kafka config relative path
     */
    public static final String KAFKA_CONFIG_RELATIVE_PATH = "etc/kafka/server.properties";

    /**
     * zookeeper tmp dir name
     */
    public static final String ZOOKEEPER_TMP_DIR_NAME = "zookeeper";

    /**
     * zookeeper starter relative path
     */
    public static final String ZOOKEEPER_STARTER_RELATIVE_PATH = "bin/zookeeper-server-start";

    /**
     * zookeeper config relative path
     */
    public static final String ZOOKEEPER_CONFIG_RELATIVE_PATH = "etc/kafka/zookeeper.properties";

    /**
     * schema registry starter relative path
     */
    public static final String SCHEMA_REGISTRY_STARTER_RELATIVE_PATH = "bin/schema-registry-start";

    /**
     * schema registry config relative path
     */
    public static final String SCHEMA_REGISTRY_CONFIG_RELATIVE_PATH = "etc/schema-registry/schema-registry.properties";

    /**
     * connect standalone relative path
     */
    public static final String CONNECT_STANDALONE_RELATIVE_PATH = "bin/connect-standalone";

    /**
     * kafka port config name
     */
    public static final String PORT_CONFIG_NAME = "kafka-port.properties";

    /**
     * kafka port config key
     */
    public static final String KAFKA_PORT_CONFIG_KEY = "kafka.port";

    /**
     * zookeeper port config key
     */
    public static final String ZOOKEEPER_PORT_CONFIG_KEY = "zookeeper.port";

    /**
     * schema registry port config key
     */
    public static final String SCHEMA_REGISTRY_PORT_CONFIG_KEY = "schema.registry.port";

    /**
     * confluent servers ip
     */
    public static final String CONFLUENT_IP = "localhost";

    /**
     * confluent url prefix
     */
    public static final String CONFLUENT_URL_PREFIX = "http://";

    private KafkaConstants() {
    }
}
