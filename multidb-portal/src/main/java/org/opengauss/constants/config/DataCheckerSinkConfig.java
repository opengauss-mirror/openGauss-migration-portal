/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * datachecker sink config
 *
 * @since 2025/5/8
 */
public class DataCheckerSinkConfig {
    /**
     * database url
     */
    public static final String DATABASE_URL = "spring.datasource.url";

    /**
     * database username
     */
    public static final String DATABASE_USERNAME = "spring.datasource.username";

    /**
     * database password
     */
    public static final String DATABASE_PASSWORD = "spring.datasource.password";

    /**
     * extract schema
     */
    public static final String EXTRACT_SCHEMA = "spring.extract.schema";

    /**
     * extract debezium enable
     */
    public static final String EXTRACT_DEBEZIUM_ENABLE = "spring.extract.debezium-enable";

    /**
     * extract debezium avro registry
     */
    public static final String EXTRACT_DEBEZIUM_AVRO_REGISTRY = "spring.extract.debezium-avro-registry";

    /**
     * extract debezium topic
     */
    public static final String EXTRACT_DEBEZIUM_TOPIC = "spring.extract.debezium-topic";

    /**
     * kafka bootstrap servers
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "spring.kafka.bootstrap-servers";

    /**
     * logging config file path
     */
    public static final String LOGGING_CONFIG = "logging.config";

    /**
     * check process uri
     */
    public static final String CHECK_SERVER_URI = "spring.check.server-uri";

    /**
     * check server port
     */
    public static final String SERVER_PORT = "server.port";

    private DataCheckerSinkConfig() {
    }
}
