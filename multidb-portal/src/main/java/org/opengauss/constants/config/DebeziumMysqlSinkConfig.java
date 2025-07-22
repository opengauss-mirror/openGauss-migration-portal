/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium mysql sink config
 *
 * @since 2025/5/7
 */
public class DebeziumMysqlSinkConfig {
    /**
     * opengauss username
     */
    public static final String OPENGAUSS_USERNAME = "opengauss.username";

    /**
     * opengauss password
     */
    public static final String OPENGAUSS_PASSWORD = "opengauss.password";

    /**
     * opengauss url
     */
    public static final String OPENGAUSS_URL = "opengauss.url";

    /**
     * schema mappings
     */
    public static final String SCHEMA_MAPPINGS = "schema.mappings";

    /**
     * opengauss standby hosts
     */
    public static final String OPENGAUSS_STANDBY_HOSTS = "database.standby.hostnames";

    /**
     * opengauss standby ports
     */
    public static final String OPENGAUSS_STANDBY_PORTS = "database.standby.ports";

    /**
     * record breakpoint kafka bootstrap servers
     */
    public static final String RECORD_BREAKPOINT_KAFKA_BOOTSTRAP_SERVERS = "record.breakpoint.kafka.bootstrap.servers";

    /**
     * debezium connect name
     */
    public static final String NAME = "name";

    /**
     * debezium connect topics
     */
    public static final String TOPICS = "topics";

    /**
     * debezium connect record breakpoint kafka topic
     */
    public static final String RECORD_BREAKPOINT_KAFKA_TOPIC = "record.breakpoint.kafka.topic";

    /**
     * debezium connect sink process file path
     */
    public static final String SINK_PROCESS_FILE_PATH = "sink.process.file.path";

    /**
     * debezium connect create count info path
     */
    public static final String CREATE_COUNT_INFO_PATH = "create.count.info.path";

    /**
     * debezium connect fail sql path
     */
    public static final String FAIL_SQL_PATH = "fail.sql.path";

    /**
     * debezium connect openGauss xlog
     */
    public static final String XLOG_LOCATION = "xlog.location";

    private DebeziumMysqlSinkConfig() {
    }
}
