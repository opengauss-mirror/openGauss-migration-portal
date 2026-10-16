/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.migration.provider;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Database migration provider, one implementation per source database type
 *
 * @since 2026/10/15
 */
public interface DatabaseMigrationProvider {
    /**
     * The database type this provider serves
     *
     * @return database type
     */
    DatabaseType databaseType();

    /**
     * Create migration job config
     *
     * @param taskWorkspace task workspace
     * @return migration job config
     */
    AbstractMigrationJobConfig createJobConfig(TaskWorkspace taskWorkspace);

    /**
     * Create migration job
     *
     * @param jobConfig migration job config
     * @return migration job
     */
    AbstractMigrationJob createJob(AbstractMigrationJobConfig jobConfig);

    /**
     * Create progress monitor
     *
     * @param statusMonitor status monitor
     * @param taskWorkspace task workspace
     * @return progress monitor
     */
    ProgressMonitor createProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace);

    /**
     * Generate migration tools config files, when create task
     *
     * @param taskWorkspace task workspace
     */
    default void generateToolsConfigFiles(TaskWorkspace taskWorkspace) {
        createJobConfig(taskWorkspace).generateToolsConfigFiles();
    }
}
