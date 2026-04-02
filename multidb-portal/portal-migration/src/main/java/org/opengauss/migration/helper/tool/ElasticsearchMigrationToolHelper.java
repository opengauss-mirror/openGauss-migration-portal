/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.tools.ElasticsearchMigrationTool;

/**
 * ElasticsearchMigrationToolHelper
 *
 * @since 2025/10/23
 */
public class ElasticsearchMigrationToolHelper {
    private ElasticsearchMigrationToolHelper() {
    }

    /**
     * Generate elasticsearch migration tool process start command.
     *
     * @param indexName index name
     * @param tableName table name
     * @return process start command
     */
    public static String generateProcessStartCommand(String indexName, String tableName) {
        StringBuilder commandBuilder = new StringBuilder("python3 ");
        ElasticsearchMigrationTool elasticsearchMigrationTool = ElasticsearchMigrationTool.getInstance();
        commandBuilder.append(elasticsearchMigrationTool.getMigrationPyPath());
        commandBuilder.append(" ").append(indexName);
        commandBuilder.append(" ").append(tableName);
        return commandBuilder.toString();
    }

    /**
     * Generate full migration log path.
     *
     * @param taskWorkspace task workspace
     * @param indexName index name
     * @return full migration log path
     */
    public static String getFullMigrationLogPath(TaskWorkspace taskWorkspace, String indexName) {
        return String.format("%s/migration_%s.log", taskWorkspace.getLogsFullDirPath(), indexName);
    }
}
