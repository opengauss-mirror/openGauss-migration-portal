/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.AbstractMigrationJobConfig;
import org.opengauss.utils.FileUtils;

import java.io.IOException;

/**
 * Migration task helper
 *
 * @since 2025/7/9
 */
public class TaskHelper {
    private TaskHelper() {
    }

    /**
     * Load source database type
     *
     * @param taskWorkspace task workspace
     * @return DatabaseType source database type
     */
    public static DatabaseType loadSourceDbType(TaskWorkspace taskWorkspace) {
        String sourceDbTypeFilePath = taskWorkspace.getSourceDbTypeFilePath();
        try {
            if (FileUtils.checkFileExists(sourceDbTypeFilePath)) {
                return DatabaseType.valueOf(FileUtils.readFileContents(sourceDbTypeFilePath).trim());
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read source database type", e);
        } catch (IllegalArgumentException e) {
            throw new MigrationException("The source database type file is abnormal. "
                    + "Please create the migration task correctly");
        }
        throw new MigrationException("The source database type file does not exist. "
                + "Please do not delete the file or modify the file name, "
                + "and do not modify the directory structure of the task");
    }

    /**
     * Change each migration phase's config
     *
     * @param migrationJobConfig migration job config
     */
    public static void changePhasesConfig(AbstractMigrationJobConfig migrationJobConfig) {
        migrationJobConfig.changeToolsConfig();
        migrationJobConfig.saveChangeConfig();
    }
}
