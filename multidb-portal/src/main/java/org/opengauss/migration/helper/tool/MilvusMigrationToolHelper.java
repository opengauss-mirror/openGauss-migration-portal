/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.tools.MilvusMigrationTool;

/**
 * MilvusMigrationToolHelper
 *
 * @since 2025/10/23
 */
public class MilvusMigrationToolHelper {
    private MilvusMigrationToolHelper() {
    }

    /**
     * Generate milvus migration tool process start command.
     *
     * @param collectionName collection name
     * @param tableName table name
     * @return process start command
     */
    public static String generateProcessStartCommand(String collectionName, String tableName) {
        StringBuilder commandBuilder = new StringBuilder("python3 ");
        MilvusMigrationTool milvusMigrationTool = MilvusMigrationTool.getInstance();
        commandBuilder.append(milvusMigrationTool.getMigrationPyPath());
        commandBuilder.append(" ").append(collectionName);
        commandBuilder.append(" ").append(tableName);
        return commandBuilder.toString();
    }

    /**
     * Generate full migration log path.
     *
     * @param taskWorkspace task workspace
     * @param collectionName collection name
     * @return full migration log path
     */
    public static String generateFullMigrationLogPath(TaskWorkspace taskWorkspace, String collectionName) {
        return String.format("%s/migration_%s.log", taskWorkspace.getLogsFullDirPath(), collectionName);
    }
}
