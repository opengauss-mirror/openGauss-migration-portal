/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * debezium constants
 *
 * @since 2025/4/19
 */
public class DebeziumConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "Debezium";

    /**
     * install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "debezium";

    /**
     * connect mysql install pkg name model
     */
    public static final String CONNECT_MYSQL_INSTALL_PKG_NAME_MODEL = "openGauss-IncReplicateMysql2OpenGauss-%s.tar.gz";

    /**
     * connect openGauss install pkg name model
     */
    public static final String CONNECT_OPENGAUSS_INSTALL_PKG_NAME_MODEL
        = "openGauss-IncReplicateOpenGauss2Mysql-%s.tar.gz";

    /**
     * connect postgresql install pkg name model
     */
    public static final String CONNECT_PGSQL_INSTALL_PKG_NAME_MODEL
        = "openGauss-IncReplicatePostgresql2OpenGauss-%s.tar.gz";

    /**
     * install dir name
     */
    public static final String INSTALL_DIR_NAME = "debezium";

    /**
     * connect mysql jar relative path
     */
    public static final String CONNECT_MYSQL_JAR_RELATIVE_PATH =
            "debezium-connector-mysql/debezium-connector-mysql-1.8.1.Final.jar";

    /**
     * connect openGauss jar relative path
     */
    public static final String CONNECT_OPENGAUSS_JAR_RELATIVE_PATH =
            "debezium-connector-opengauss/debezium-connector-opengauss-1.8.1.Final.jar";

    /**
     * connect postgresql jar relative path
     */
    public static final String CONNECT_PGSQL_JAR_RELATIVE_PATH =
            "debezium-connector-postgres/debezium-connector-postgres-1.8.1.Final.jar";

    /**
     * wait process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 3000;

    /**
     * source process status file name prefix
     */
    public static final String INCREMENTAL_SOURCE_STATUS_FILE_PREFIX = "forward-source-process";

    /**
     * sink process status file name prefix
     */
    public static final String INCREMENTAL_SINK_STATUS_FILE_PREFIX = "forward-sink-process";

    /**
     * reverse source process status file name prefix
     */
    public static final String REVERSE_SOURCE_STATUS_FILE_PREFIX = "reverse-source-process";

    /**
     * reverse sink process status file name prefix
     */
    public static final String REVERSE_SINK_STATUS_FILE_PREFIX = "reverse-sink-process";

    /**
     * fail sql file name
     */
    public static final String FAIL_SQL_FILE_NAME = "fail-sql.txt";

    private DebeziumConstants() {
    }
}
