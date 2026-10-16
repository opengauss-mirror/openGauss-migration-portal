/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.migration.provider;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.ElasticsearchMigrationJob;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.job.config.ElasticsearchMigrationJobConfig;
import org.opengauss.migration.progress.ElasticsearchProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Elasticsearch migration provider
 *
 * @since 2026/10/15
 */
public class ElasticsearchMigrationProvider implements DatabaseMigrationProvider {
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.ELASTICSEARCH;
    }

    @Override
    public AbstractMigrationJobConfig createJobConfig(TaskWorkspace taskWorkspace) {
        return new ElasticsearchMigrationJobConfig(taskWorkspace);
    }

    @Override
    public AbstractMigrationJob createJob(AbstractMigrationJobConfig jobConfig) {
        if (!(jobConfig instanceof ElasticsearchMigrationJobConfig)) {
            throw new IllegalArgumentException("Invalid job config type for ElasticsearchMigrationProvider");
        }
        return new ElasticsearchMigrationJob((ElasticsearchMigrationJobConfig) jobConfig);
    }

    @Override
    public ProgressMonitor createProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        return new ElasticsearchProgressMonitor(statusMonitor, taskWorkspace);
    }
}
