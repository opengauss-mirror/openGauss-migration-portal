/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * Milvus migration tool constants
 *
 * @since 2025/10/20
 */
public class MilvusMigrationToolConstants {
    /**
     * Tool name
     */
    public static final String TOOL_NAME = "Milvus_migration_tool";

    /**
     * Install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "milvus";

    /**
     * Milvus migration python script name
     */
    public static final String MIGRATION_PY_NAME = "milvus2datavec.py";

    /**
     * Install dir name
     */
    public static final String INSTALL_DIR_NAME = "milvus";

    /**
     * Wait Milvus migration process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 100;

    /**
     * Milvus migration end flag
     */
    public static final String MIGRATION_END_FLAG = "Migration end.";

    /**
     * Milvus migration failed flag
     */
    public static final String MIGRATION_FAILED_FLAG = "Migration failed";

    /**
     * Milvus migration success flag
     */
    public static final String MIGRATION_SUCCESS_FLAG = "Migration completed in";

    private MilvusMigrationToolConstants() {
    }
}
