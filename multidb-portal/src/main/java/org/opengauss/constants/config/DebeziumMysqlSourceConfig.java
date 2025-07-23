/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * debezium mysql source config
 *
 * @since 2025/5/7
 */
public class DebeziumMysqlSourceConfig {
    /**
     * database hostname
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
     * database history kafka bootstrap servers
     */
    public static final String DATABASE_HISTORY_KAFKA_SERVERS = "database.history.kafka.bootstrap.servers";

    /**
     * database password
     */
    public static final String DATABASE_PASSWORD = "database.password";

    /**
     * debezium connector name
     */
    public static final String NAME = "name";

    /**
     * transforms route regex
     */
    public static final String TRANSFORMS_ROUTE_REGEX = "transforms.route.regex";

    /**
     * database server name
     */
    public static final String DATABASE_SERVER_NAME = "database.server.name";

    /**
     * database server id
     */
    public static final String DATABASE_SERVER_ID = "database.server.id";

    /**
     * database history kafka topic
     */
    public static final String DATABASE_HISTORY_KAFKA_TOPIC = "database.history.kafka.topic";

    /**
     * transforms route replacement
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
     * snapshot offset binlog filename
     */
    public static final String SNAPSHOT_OFFSET_BINLOG_FILENAME = "snapshot.offset.binlog.filename";

    /**
     * snapshot offset binlog position
     */
    public static final String SNAPSHOT_OFFSET_BINLOG_POSITION = "snapshot.offset.binlog.position";

    /**
     * snapshot offset gtid set
     */
    public static final String SNAPSHOT_OFFSET_GTID_SET = "snapshot.offset.gtid.set";

    /**
     * kafka bootstrap servers
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.server";

    /**
     * database include list
     */
    public static final String DATABASE_INCLUDE_LIST = "database.include.list";

    /**
     * database table include list
     */
    public static final String TABLE_INCLUDE_LIST = "table.include.list";

    private DebeziumMysqlSourceConfig() {
    }
}
