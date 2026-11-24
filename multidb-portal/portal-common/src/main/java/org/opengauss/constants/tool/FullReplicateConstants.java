/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.constants.tool;

/**
 * oG_datasync_full_migration constants
 *
 * @since 2025/5/29
 */
public class FullReplicateConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "openGauss-FullReplicate";

    /**
     * install package directory name
     */
    public static final String INSTALL_PKG_DIR_NAME = "full_replicate";

    /**
     * install package name
     */
    public static final String INSTALL_PKG_NAME = "openGauss-FullReplicate-%s.tar.gz";

    /**
     * install directory name
     */
    public static final String INSTALL_DIR_NAME = "full_replicate";

    /**
     * oG_datasync_full_migration jar name model
     */
    public static final String FULL_REPLICATE_JAR_NAME_MODEL = "openGauss-FullReplicate-%s.jar";

    /**
     * oG_datasync_full_migration jar name
     */
    public static final String FULL_REPLICATE_JAR_HOME_NAME = "openGauss-FullReplicate";

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

    private FullReplicateConstants() {
    }
}
