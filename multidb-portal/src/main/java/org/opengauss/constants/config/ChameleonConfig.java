/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * Chameleon config
 *
 * @since 2025/5/6
 */
public class ChameleonConfig {
    /**
     * pg database ip
     */
    public static final String PG_DATABASE_IP = "pg_conn.host";

    /**
     * pg database port
     */
    public static final String PG_DATABASE_PORT = "pg_conn.port";

    /**
     * pg database user
     */
    public static final String PG_DATABASE_USER = "pg_conn.user";

    /**
     * pg database password
     */
    public static final String PG_DATABASE_PASSWORD = "pg_conn.password";

    /**
     * pg database name
     */
    public static final String PG_DATABASE_NAME = "pg_conn.database";

    /**
     * mysql database ip
     */
    public static final String MYSQL_DATABASE_IP = "sources.mysql.db_conn.host";

    /**
     * mysql database port
     */
    public static final String MYSQL_DATABASE_PORT = "sources.mysql.db_conn.port";

    /**
     * mysql database user
     */
    public static final String MYSQL_DATABASE_USER = "sources.mysql.db_conn.user";

    /**
     * mysql database password
     */
    public static final String MYSQL_DATABASE_PASSWORD = "sources.mysql.db_conn.password";

    /**
     * mysql database name
     */
    public static final String MYSQL_DATABASE_NAME = "sources.mysql.db_conn.database";

    /**
     * mysql schema mappings
     */
    public static final String MYSQL_SCHEMA_MAPPINGS = "sources.mysql.schema_mappings";

    /**
     * mysql limit tables
     */
    public static final String MYSQL_LIMIT_TABLES = "sources.mysql.limit_tables";

    /**
     * mysql csv dir
     */
    public static final String MYSQL_CSV_DIR = "sources.mysql.csv_dir";

    /**
     * mysql out dir
     */
    public static final String MYSQL_OUT_DIR = "sources.mysql.out_dir";

    /**
     * pid dir
     */
    public static final String PID_DIR = "pid_dir";

    /**
     * dump json
     */
    public static final String DUMP_JSON = "dump_json";

    /**
     * log level
     */
    public static final String LOG_LEVEL = "log_level";

    /**
     * alert log collection enable
     */
    public static final String ALERT_LOG_COLLECTION_ENABLE = "alert_log_collection_enable";

    /**
     * alert log kafka server
     */
    public static final String ALERT_LOG_KAFKA_SERVER = "alert_log_kafka_server";

    /**
     * alert log kafka topic
     */
    public static final String ALERT_LOG_KAFKA_TOPIC = "alert_log_kafka_topic";

    private ChameleonConfig() {
    }
}
