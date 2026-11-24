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

package org.opengauss.migration.provider;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.PgsqlMigrationJob;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.job.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.progress.PgsqlProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Pgsql migration provider
 *
 * @since 2026/10/15
 */
public class PgsqlMigrationProvider implements DatabaseMigrationProvider {
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    public AbstractMigrationJobConfig createJobConfig(TaskWorkspace taskWorkspace) {
        return new PgsqlMigrationJobConfig(taskWorkspace);
    }

    @Override
    public AbstractMigrationJob createJob(AbstractMigrationJobConfig jobConfig) {
        if (!(jobConfig instanceof PgsqlMigrationJobConfig)) {
            throw new IllegalArgumentException("Invalid job config type for PgsqlMigrationProvider");
        }
        return new PgsqlMigrationJob((PgsqlMigrationJobConfig) jobConfig);
    }

    @Override
    public ProgressMonitor createProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        return new PgsqlProgressMonitor(statusMonitor, taskWorkspace);
    }
}
