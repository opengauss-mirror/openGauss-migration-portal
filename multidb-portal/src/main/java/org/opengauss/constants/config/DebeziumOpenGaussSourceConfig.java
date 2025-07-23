/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium openGauss source config
 *
 * @since 2025/5/8
 */
public class DebeziumOpenGaussSourceConfig {
    /**
     * database ip
     */
    public static final String DATABASE_HOSTNAME = "database.hostname";

    /**
     * database port
     */
    public static final String DATABASE_PORT = "database.port";

    /**
     * database username
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
     * include table list
     */
    public static final String TABLE_INCLUDE_LIST = "table.include.list";

    /**
     * include schema list
     */
    public static final String SCHEMA_INCLUDE_LIST = "schema.include.list";

    /**
     * database is cluster
     */
    public static final String DATABASE_IS_CLUSTER = "database.iscluster";

    /**
     * database standby hostnames
     */
    public static final String DATABASE_STANDBY_HOSTNAMES = "database.standby.hostnames";

    /**
     * database standby ports
     */
    public static final String DATABASE_STANDBY_PORTS = "database.standby.ports";

    /**
     * debezium source connector name
     */
    public static final String NAME = "name";

    /**
     * database server name
     */
    public static final String DATABASE_SERVER_NAME = "database.server.name";

    /**
     * database history kafka topic
     */
    public static final String DATABASE_HISTORY_KAFKA_TOPIC = "database.history.kafka.topic";

    /**
     * transform route regex
     */
    public static final String TRANSFORMS_ROUTE_REGEX = "transforms.route.regex";

    /**
     * transform route replacement
     */
    public static final String TRANSFORMS_ROUTE_REPLACEMENT = "transforms.route.replacement";

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
     * debezium connect openGauss xlog
     */
    public static final String XLOG_LOCATION = "xlog.location";

    /**
     * debezium plugin name
     */
    public static final String PLUGIN_NAME = "plugin.name";

    /**
     * publication auto create mode
     */
    public static final String PUBLICATION_AUTO_CREATE_MODE = "publication.autocreate.mode";

    private DebeziumOpenGaussSourceConfig() {
    }
}
