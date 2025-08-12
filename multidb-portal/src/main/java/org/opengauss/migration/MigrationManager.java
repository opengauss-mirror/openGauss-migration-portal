/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration;

import org.opengauss.Main;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.AbstractMigrationJobConfig;
import org.opengauss.migration.config.MysqlMigrationJobConfig;
import org.opengauss.migration.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.MysqlMigrationJob;
import org.opengauss.migration.job.PgsqlMigrationJob;
import org.opengauss.migration.monitor.MigrationAliveMonitor;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitorFactory;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Migration manager
 *
 * @since 2025/7/3
 */
public class MigrationManager {
    private static volatile MigrationManager instance;

    private TaskWorkspace taskWorkspace;
    private DatabaseType sourceDbType;
    private AbstractMigrationJobConfig migrationJobConfig;
    private MigrationStopIndicator migrationStopIndicator;
    private ProgressMonitor progressMonitor;
    private ProcessMonitor processMonitor;
    private StatusMonitor statusMonitor;
    private MigrationAliveMonitor migrationAliveMonitor;
    private AbstractMigrationJob migrationJob;

    private MigrationManager() {
    }

    /**
     * Initialize migration context
     *
     * @param taskWorkspace task workspace
     */
    public static void initialize(TaskWorkspace taskWorkspace) {
        if (instance == null) {
            synchronized (MigrationManager.class) {
                if (instance == null) {
                    initMigrationContext(taskWorkspace);
                }
            }
        } else {
            throw new IllegalStateException("Migration context already initialized");
        }
    }

    /**
     * Get migration manager
     *
     * @return MigrationManager migration manager
     */
    public static MigrationManager getInstance() {
        if (instance == null) {
            synchronized (MigrationManager.class) {
                if (instance == null) {
                    throw new IllegalStateException("Migration context has not initialized");
                }
            }
        }
        return instance;
    }

    /**
     * Start migration
     */
    public void start() {
        statusMonitor.setCurrentStatus(MigrationStatusEnum.MIGRATION_STARTING);
        if (!migrationJob.preMigrationVerify()) {
            migrationStopIndicator.setStop();
            statusMonitor.setCurrentStatus(MigrationStatusEnum.PRE_MIGRATION_VERIFY_FAILED);
            Main.stopQuarkus();
            return;
        }

        startMonitor();
        migrationJob.beforeTask();
        migrationJob.startTask(migrationStopIndicator, processMonitor, statusMonitor);

        if (!migrationJobConfig.hasIncrementalMigration() && !migrationJobConfig.hasReverseMigration()) {
            Main.stopQuarkus();
        }
    }

    /**
     * Stop migration
     */
    public void stop() {
        if (!migrationStopIndicator.isStopped()) {
            statusMonitor.setCurrentStatus(MigrationStatusEnum.MIGRATION_STOPPING);
            doStop();
            statusMonitor.setCurrentStatus(MigrationStatusEnum.MIGRATION_FINISHED);
        }
    }

    /**
     * Stop migration on error
     */
    public void stopOnError() {
        if (!migrationStopIndicator.isStopped()) {
            statusMonitor.setCurrentStatus(MigrationStatusEnum.MIGRATION_FAILED);
            doStop();
        }
    }

    /**
     * Stop incremental migration
     */
    public void stopIncremental() {
        migrationJob.stopIncremental(migrationStopIndicator, statusMonitor);
    }

    /**
     * Resume incremental migration
     */
    public void resumeIncremental() {
        migrationJob.resumeIncremental(statusMonitor);
    }

    /**
     * Restart incremental migration
     */
    public void restartIncremental() {
        migrationJob.restartIncremental(migrationStopIndicator, statusMonitor);
    }

    /**
     * Start reverse migration
     */
    public void startReverse() {
        migrationJob.startReverse(migrationStopIndicator, statusMonitor);
    }

    /**
     * Stop reverse migration
     */
    public void stopReverse() {
        migrationJob.stopReverse(statusMonitor);
    }

    /**
     * Resume reverse migration
     */
    public void resumeReverse() {
        migrationJob.resumeReverse(statusMonitor);
    }

    /**
     * Restart reverse migration
     */
    public void restartReverse() {
        migrationJob.restartReverse(migrationStopIndicator, statusMonitor);
    }

    private void doStop() {
        migrationStopIndicator.setStop();
        migrationJob.stopTask();
        stopMonitor();
    }

    private void stopMonitor() {
        processMonitor.stopMonitoring();
        progressMonitor.stopMonitoring();
        migrationAliveMonitor.stop();
    }

    private void startMonitor() {
        processMonitor.startMonitoring(this, statusMonitor);
        progressMonitor.start();
        migrationAliveMonitor.start();
    }

    private static void initMigrationContext(TaskWorkspace taskWorkspace) {
        MigrationManager migrationManager = new MigrationManager();
        DatabaseType sourceDbType = TaskHelper.loadSourceDbType(taskWorkspace);
        migrationManager.taskWorkspace = taskWorkspace;
        migrationManager.sourceDbType = sourceDbType;

        if (DatabaseType.MYSQL.equals(sourceDbType)) {
            MysqlMigrationJobConfig migrationJobConfig = new MysqlMigrationJobConfig(taskWorkspace);
            TaskHelper.loadConfig(migrationJobConfig);
            migrationManager.migrationJobConfig = migrationJobConfig;
            migrationManager.migrationJob = new MysqlMigrationJob(migrationJobConfig);
        } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
            PgsqlMigrationJobConfig migrationJobConfig = new PgsqlMigrationJobConfig(taskWorkspace);
            TaskHelper.loadConfig(migrationJobConfig);
            migrationManager.migrationJobConfig = migrationJobConfig;
            migrationManager.migrationJob = new PgsqlMigrationJob(migrationJobConfig);
        } else {
            throw new MigrationException("Unsupported source database type: " + sourceDbType);
        }

        StatusMonitor statusMonitor = new StatusMonitor(taskWorkspace);
        migrationManager.statusMonitor = statusMonitor;
        migrationManager.progressMonitor = ProgressMonitorFactory.createProgressMonitor(
                sourceDbType, statusMonitor, taskWorkspace);
        migrationManager.migrationStopIndicator = new MigrationStopIndicator();
        migrationManager.migrationAliveMonitor = new MigrationAliveMonitor(taskWorkspace);
        migrationManager.processMonitor = new ProcessMonitor();

        instance = migrationManager;
    }
}
