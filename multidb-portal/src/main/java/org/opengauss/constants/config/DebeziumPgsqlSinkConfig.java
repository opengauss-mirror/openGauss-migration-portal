/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium pgsql sink config
 *
 * @since 2025/6/10
 */
public class DebeziumPgsqlSinkConfig {
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
     * debezium sink connector name
     */
    public static final String NAME = "name";

    /**
     * kafka topic
     */
    public static final String TOPICS = "topics";

    /**
     * record breakpoint kafka topic
     */
    public static final String COMMIT_PROCESS_WHILE_RUNNING = "commit.process.while.running";

    /**
     * sink process file path
     */
    public static final String SINK_PROCESS_FILE_PATH = "sink.process.file.path";

    /**
     * create count info path
     */
    public static final String CREATE_COUNT_INFO_PATH = "create.count.info.path";

    /**
     * fail sql path
     */
    public static final String FAIL_SQL_PATH = "fail.sql.path";

    /**
     * xlog location save path
     */
    public static final String XLOG_LOCATION = "xlog.location";

    private DebeziumPgsqlSinkConfig() {
    }
}
