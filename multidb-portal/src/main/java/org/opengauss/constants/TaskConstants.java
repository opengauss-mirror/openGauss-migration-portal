/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

import org.opengauss.enums.DatabaseType;

import java.util.List;

/**
 * task constants
 *
 * @since 2025/4/28
 */
public class TaskConstants {
    /**
     * max task id length
     */
    public static final int MAX_TASK_ID_LENGTH = 50;

    /**
     * task id verify pattern
     */
    public static final String TASK_ID_PATTERN = "^[a-zA-Z0-9_-]+$";

    /**
     * supported source db types
     */
    public static final List<DatabaseType> SUPPORTED_SOURCE_DB_TYPES = List.of(
            DatabaseType.MYSQL,
            DatabaseType.POSTGRESQL
    );

    /**
     * task workspace dir suffix
     */
    public static final String TASK_WORKSPACE_DIR_SUFFIX = "task_";

    /**
     * source db type config file name
     */
    public static final String SOURCE_DB_TYPE_CONFIG_FILE_NAME = "source-database-type";

    /**
     * quarkus port file name
     */
    public static final String QUARKUS_PORT_FILE_NAME = "port";

    /**
     * migration heartbeat file name
     */
    public static final String HEARTBEAT_FILE = "migration.heartbeat";

    private TaskConstants() {
    }
}
