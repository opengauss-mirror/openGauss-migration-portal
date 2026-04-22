/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * Elasticsearch migration tool constants
 *
 * @since 2025/10/20
 */
public class ElasticsearchMigrationToolConstants {
    /**
     * Tool name
     */
    public static final String TOOL_NAME = "Elasticsearch_migration_tool";

    /**
     * Install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "elasticsearch";

    /**
     * Elasticsearch migration python script name
     */
    public static final String MIGRATION_PY_NAME = "elasticsearch2datavec.py";

    /**
     * Install dir name
     */
    public static final String INSTALL_DIR_NAME = "elasticsearch";

    /**
     * Wait Elasticsearch migration process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 100;

    /**
     * Elasticsearch migration end flag
     */
    public static final String MIGRATION_END_FLAG = "Migration end.";

    /**
     * Elasticsearch migration failed flag
     */
    public static final String MIGRATION_FAILED_FLAG = "Migration failed";

    /**
     * Elasticsearch migration success flag
     */
    public static final String MIGRATION_SUCCESS_FLAG = "Successfully inserted data into table";

    private ElasticsearchMigrationToolConstants() {
    }
}
