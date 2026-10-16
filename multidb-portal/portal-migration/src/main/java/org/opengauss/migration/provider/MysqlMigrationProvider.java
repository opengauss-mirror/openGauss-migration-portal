/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.migration.provider;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.MysqlMigrationJob;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.job.config.MysqlMigrationJobConfig;
import org.opengauss.migration.progress.MysqlProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Mysql migration provider
 *
 * @since 2026/10/15
 */
public class MysqlMigrationProvider implements DatabaseMigrationProvider {
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.MYSQL;
    }

    @Override
    public AbstractMigrationJobConfig createJobConfig(TaskWorkspace taskWorkspace) {
        return new MysqlMigrationJobConfig(taskWorkspace);
    }

    @Override
    public AbstractMigrationJob createJob(AbstractMigrationJobConfig jobConfig) {
        if (!(jobConfig instanceof MysqlMigrationJobConfig)) {
            throw new IllegalArgumentException("Invalid job config type for MysqlMigrationProvider");
        }
        return new MysqlMigrationJob((MysqlMigrationJobConfig) jobConfig);
    }

    @Override
    public ProgressMonitor createProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        return new MysqlProgressMonitor(statusMonitor, taskWorkspace);
    }
}
