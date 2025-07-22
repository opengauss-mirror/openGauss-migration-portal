/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * full migration tool constants
 *
 * @since 2025/5/29
 */
public class FullMigrationToolConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "Full-Migration";

    /**
     * install package directory name
     */
    public static final String INSTALL_PKG_DIR_NAME = "full-migration";

    /**
     * install package name
     */
    public static final String INSTALL_PKG_NAME = "full-migration-tool-%s.tar.gz";

    /**
     * install directory name
     */
    public static final String INSTALL_DIR_NAME = "full-migration";

    /**
     * full migration jar name model
     */
    public static final String FULL_MIGRATION_JAR_NAME_MODEL = "full-migration-tool-%s.jar";

    /**
     * full migration jar name
     */
    public static final String FULL_MIGRATION_JAR_HOME_NAME = "full-migration-tool";

    /**
     * wait process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 2000;

    /**
     * order table
     */
    public static final String ORDER_TABLE = "table";

    /**
     * order sequence
     */
    public static final String ORDER_SEQUENCE = "sequence";

    /**
     * order primary key
     */
    public static final String ORDER_PRIMARY_KEY = "primarykey";

    /**
     * order index
     */
    public static final String ORDER_INDEX = "index";

    /**
     * order constraint
     */
    public static final String ORDER_CONSTRAINT = "constraint";

    /**
     * order view
     */
    public static final String ORDER_VIEW = "view";

    /**
     * order function
     */
    public static final String ORDER_FUNCTION = "function";

    /**
     * order procedure
     */
    public static final String ORDER_PROCEDURE = "procedure";

    /**
     * order trigger
     */
    public static final String ORDER_TRIGGER = "trigger";

    /**
     * order foreignkey
     */
    public static final String ORDER_FOREIGN_KEY = "foreignkey";

    /**
     * order drop_replica_schema
     */
    public static final String ORDER_DROP_REPLICA_SCHEMA = "drop_replica_schema";

    /**
     * support source db type: postgresql
     */
    public static final String SUPPORT_SOURCE_DB_TYPE_PGSQL = "postgresql";

    private FullMigrationToolConstants() {
    }
}
