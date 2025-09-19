/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * migration config
 *
 * @since 2025/4/30
 */
public class MigrationConfig {
    /**
     * Migration mode
     */
    public static final String MIGRATION_MODE = "migration.mode";

    /**
     * Whether to migrate objects. Default value is true
     */
    public static final String IS_MIGRATION_OBJECT = "is.migration.object";

    /**
     * Whether adjust openGauss kernel parameters. Default value is false.
     */
    public static final String IS_ADJUST_KERNEL_PARAM = "is.adjust.kernel.param";

    /**
     * MySQL server IP address
     */
    public static final String MYSQL_DATABASE_IP = "mysql.database.ip";

    /**
     * MySQL server port
     */
    public static final String MYSQL_DATABASE_PORT = "mysql.database.port";

    /**
     * MySQL database name
     */
    public static final String MYSQL_DATABASE_NAME = "mysql.database.name";

    /**
     * MySQL server user name
     */
    public static final String MYSQL_DATABASE_USERNAME = "mysql.database.username";

    /**
     * MySQL server user password
     */
    public static final String MYSQL_DATABASE_PASSWORD = "mysql.database.password";

    /**
     * MySQL tables to be migrated
     */
    public static final String MYSQL_DATABASE_TABLES = "mysql.database.tables";

    /**
     * PostgreSQL server IP address
     */
    public static final String PGSQL_DATABASE_IP = "pgsql.database.ip";

    /**
     * PostgreSQL server port
     */
    public static final String PGSQL_DATABASE_PORT = "pgsql.database.port";

    /**
     * PostgreSQL database name
     */
    public static final String PGSQL_DATABASE_NAME = "pgsql.database.name";

    /**
     * PostgreSQL server user name
     */
    public static final String PGSQL_DATABASE_USERNAME = "pgsql.database.username";

    /**
     * PostgreSQL server user password
     */
    public static final String PGSQL_DATABASE_PASSWORD = "pgsql.database.password";

    /**
     * PostgreSQL schemas to be migrated
     */
    public static final String PGSQL_DATABASE_SCHEMAS = "pgsql.database.schemas";

    /**
     * OpenGauss server IP address
     */
    public static final String OPENGAUSS_DATABASE_IP = "opengauss.database.ip";

    /**
     * OpenGauss server port
     */
    public static final String OPENGAUSS_DATABASE_PORT = "opengauss.database.port";

    /**
     * OpenGauss database name
     */
    public static final String OPENGAUSS_DATABASE_NAME = "opengauss.database.name";

    /**
     * OpenGauss server user name
     */
    public static final String OPENGAUSS_DATABASE_USERNAME = "opengauss.database.username";

    /**
     * OpenGauss server user password
     */
    public static final String OPENGAUSS_DATABASE_PASSWORD = "opengauss.database.password";

    /**
     * OpenGauss schema of the migration
     */
    public static final String OPENGAUSS_DATABASE_SCHEMA = "opengauss.database.schema";

    /**
     * OpenGauss database standby nodes ip
     */
    public static final String OPENGAUSS_DATABASE_STANDBY_HOSTS = "opengauss.database.standby.hosts";

    /**
     * OpenGauss database standby nodes port
     */
    public static final String OPENGAUSS_DATABASE_STANDBY_PORTS = "opengauss.database.standby.ports";

    /**
     * Schema mappings
     */
    public static final String SCHEMA_MAPPINGS = "schema.mappings";

    /**
     * Full migration process JVM configuration
     */
    public static final String FULL_PROCESS_JVM = "full.process.jvm";

    /**
     * Full data check source process JVM configuration
     */
    public static final String FULL_CHECK_SOURCE_PROCESS_JVM = "full.check.source.jvm";

    /**
     * Full data check sink process JVM configuration
     */
    public static final String FULL_CHECK_SINK_PROCESS_JVM = "full.check.sink.jvm";

    /**
     * Full data check process JVM configuration
     */
    public static final String FULL_CHECK_CHECK_PROCESS_JVM = "full.check.jvm";

    /**
     * Incremental data check source process JVM configuration
     */
    public static final String INCREMENTAL_CHECK_SOURCE_PROCESS_JVM = "incremental.check.source.jvm";

    /**
     * Incremental data check sink process JVM configuration
     */
    public static final String INCREMENTAL_CHECK_SINK_PROCESS_JVM = "incremental.check.sink.jvm";

    /**
     * Incremental data check process JVM configuration
     */
    public static final String INCREMENTAL_CHECK_CHECK_PROCESS_JVM = "incremental.check.jvm";

    /**
     * Incremental migration source process JVM configuration
     */
    public static final String INCREMENTAL_MIGRATION_SOURCE_PROCESS_JVM = "incremental.source.jvm";

    /**
     * Incremental migration sink process JVM configuration
     */
    public static final String INCREMENTAL_MIGRATION_SINK_PROCESS_JVM = "incremental.sink.jvm";

    /**
     * Reverse migration source process JVM configuration
     */
    public static final String REVERSE_MIGRATION_SOURCE_PROCESS_JVM = "reverse.source.jvm";

    /**
     * Reverse migration sink process JVM configuration
     */
    public static final String REVERSE_MIGRATION_SINK_PROCESS_JVM = "reverse.sink.jvm";

    /**
     * Whether to use interactive password input
     */
    public static final String USE_INTERACTIVE_PASSWORD = "use.interactive.password";

    private MigrationConfig() {
    }
}
