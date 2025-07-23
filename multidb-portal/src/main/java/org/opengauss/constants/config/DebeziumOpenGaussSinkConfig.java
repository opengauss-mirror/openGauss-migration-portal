/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium openGauss sink config
 *
 * @since 2025/5/8
 */
public class DebeziumOpenGaussSinkConfig {
    /**
     * database type
     */
    public static final String DATABASE_TYPE = "database.type";

    /**
     * database username
     */
    public static final String DATABASE_USERNAME = "database.username";

    /**
     * database password
     */
    public static final String DATABASE_PASSWORD = "database.password";

    /**
     * database name
     */
    public static final String DATABASE_NAME = "database.name";

    /**
     * database port
     */
    public static final String DATABASE_PORT = "database.port";

    /**
     * database ip
     */
    public static final String DATABASE_IP = "database.ip";

    /**
     * schema mappings
     */
    public static final String SCHEMA_MAPPINGS = "schema.mappings";

    /**
     * table include list
     */
    public static final String TABLE_INCLUDE_LIST = "table.include.list";

    /**
     * debezium sink connect name
     */
    public static final String NAME = "name";

    /**
     * debezium sink topics
     */
    public static final String TOPICS = "topics";

    /**
     * debezium sink record breakpoint kafka topic
     */
    public static final String RECORD_BREAKPOINT_KAFKA_TOPIC = "record.breakpoint.kafka.topic";

    /**
     * debezium sink record breakpoint kafka bootstrap servers
     */
    public static final String RECORD_BREAKPOINT_KAFKA_BOOTSTRAP_SERVERS = "record.breakpoint.kafka.bootstrap.servers";

    /**
     * debezium sink process file path
     */
    public static final String SINK_PROCESS_FILE_PATH = "sink.process.file.path";

    /**
     * debezium sink create count info path
     */
    public static final String CREATE_COUNT_INFO_PATH = "create.count.info.path";

    /**
     * debezium sink fail sql path
     */
    public static final String FAIL_SQL_PATH = "fail.sql.path";

    private DebeziumOpenGaussSinkConfig() {
    }
}
