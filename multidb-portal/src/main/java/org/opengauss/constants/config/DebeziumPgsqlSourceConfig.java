/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium pgsql source config
 *
 * @since 2025/6/10
 */
public class DebeziumPgsqlSourceConfig {
    /**
     * database ip
     */
    public static final String DATABASE_HOSTNAME = "database.hostname";

    /**
     * database port
     */
    public static final String DATABASE_PORT = "database.port";

    /**
     * database user
     */
    public static final String DATABASE_USER = "database.user";

    /**
     * database password
     */
    public static final String DATABASE_PASSWORD = "database.password";

    /**
     * database name
     */
    public static final String DATABASE_NAME = "database.dbname";

    /**
     * schema include list
     */
    public static final String SCHEMA_INCLUDE_LIST = "schema.include.list";

    /**
     * table include list
     */
    public static final String TABLE_INCLUDE_LIST = "table.include.list";

    /**
     * schema exclude list
     */
    public static final String SCHEMA_EXCLUDE_LIST = "schema.exclude.list";

    /**
     * table exclude list
     */
    public static final String TABLE_EXCLUDE_LIST = "table.exclude.list";

    /**
     * debezium connect name
     */
    public static final String NAME = "name";

    /**
     * database server name
     */
    public static final String DATABASE_SERVER_NAME = "database.server.name";

    /**
     * transforms route regex
     */
    public static final String TRANSFORMS_ROUTE_REGEX = "transforms.route.regex";

    /**
     * transforms route replacement
     */
    public static final String TRANSFORMS_ROUTE_REPLACEMENT = "transforms.route.replacement";

    /**
     * commit process while running
     */
    public static final String COMMIT_PROCESS_WHILE_RUNNING = "commit.process.while.running";

    /**
     * source process file path
     */
    public static final String SOURCE_PROCESS_FILE_PATH = "source.process.file.path";

    /**
     * create count info path
     */
    public static final String CREATE_COUNT_INFO_PATH = "create.count.info.path";

    /**
     * database slot name
     */
    public static final String SLOT_NAME = "slot.name";

    /**
     * database slot drop on stop
     */
    public static final String SLOT_DROP_ON_STOP = "slot.drop.on.stop";

    /**
     * plugin name
     */
    public static final String PLUGIN_NAME = "plugin.name";

    /**
     * migration type
     */
    public static final String MIGRATION_TYPE = "migration.type";

    /**
     * truncate handling mode
     */
    public static final String TRUNCATE_HANDLING_MODE = "truncate.handling.mode";

    private DebeziumPgsqlSourceConfig() {
    }
}
