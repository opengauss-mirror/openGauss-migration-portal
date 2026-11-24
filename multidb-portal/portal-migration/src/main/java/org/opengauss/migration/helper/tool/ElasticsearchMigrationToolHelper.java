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
