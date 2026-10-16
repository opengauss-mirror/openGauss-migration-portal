/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration;

import lombok.Getter;
import org.opengauss.migration.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.monitor.MigrationAliveMonitor;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.provider.DatabaseMigrationProvider;
import org.opengauss.migration.provider.MigrationProviderRegistry;
import org.opengauss.migration.status.StatusMonitor;

/**
 * MigrationContext
 *
 * @since 2025/9/15
 */
@Getter
public class MigrationContext {
    private static volatile MigrationContext instance;

    private TaskWorkspace taskWorkspace;
    private DatabaseType sourceDbType;

    private MigrationStopIndicator migrationStopIndicator;

    private ProgressMonitor progressMonitor;
    private ProcessMonitor processMonitor;
    private StatusMonitor statusMonitor;
    private MigrationAliveMonitor migrationAliveMonitor;

    private AbstractMigrationJobConfig migrationJobConfig;
    private AbstractMigrationJob migrationJob;

    private MigrationContext() {
    }

    /**
     * Get migration context
     *
     * @return MigrationContext migration context
     */
    public static MigrationContext getInstance() {
        if (instance == null) {
            synchronized (MigrationContext.class) {
                if (instance == null) {
                    throw new IllegalStateException("Migration context has not initialized");
                }
            }
        }
        return instance;
    }

    /**
     * Initialize migration context
     *
     * @param taskWorkspace task workspace
     */
    public static void initialize(TaskWorkspace taskWorkspace) {
        if (instance == null) {
            synchronized (MigrationContext.class) {
                if (instance == null) {
                    initMigrationContext(taskWorkspace);
                }
            }
        } else {
            throw new IllegalStateException("Migration context already initialized");
        }
    }

    private static void initMigrationContext(TaskWorkspace taskWorkspace) {
        instance = new MigrationContext();
        instance.taskWorkspace = taskWorkspace;
        DatabaseType sourceDbType = TaskHelper.loadSourceDbType(taskWorkspace);
        instance.sourceDbType = sourceDbType;

        StatusMonitor statusMonitor = new StatusMonitor(taskWorkspace);
        instance.statusMonitor = statusMonitor;
        DatabaseMigrationProvider migrationProvider = MigrationProviderRegistry.getProvider(sourceDbType);
        instance.progressMonitor = migrationProvider.createProgressMonitor(statusMonitor, taskWorkspace);
        instance.migrationStopIndicator = new MigrationStopIndicator();
        instance.migrationAliveMonitor = new MigrationAliveMonitor(taskWorkspace);
        instance.processMonitor = new ProcessMonitor();

        AbstractMigrationJobConfig migrationJobConfig = migrationProvider.createJobConfig(taskWorkspace);
        migrationJobConfig.loadConfig();
        instance.migrationJobConfig = migrationJobConfig;
        instance.migrationJob = migrationProvider.createJob(migrationJobConfig);
    }
}
