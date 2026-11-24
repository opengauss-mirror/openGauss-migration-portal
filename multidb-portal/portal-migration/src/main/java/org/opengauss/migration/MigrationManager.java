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

package org.opengauss.migration;

import org.opengauss.migration.domain.model.MigrationStopIndicator;
import org.opengauss.migration.enums.MigrationStatusEnum;
import org.opengauss.migration.job.config.AbstractMigrationJobConfig;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.monitor.MigrationAliveMonitor;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.status.StatusMonitor;

/**
 * Migration manager
 *
 * @since 2025/7/3
 */
public class MigrationManager {
    private static volatile MigrationManager instance;

    private AbstractMigrationJobConfig migrationJobConfig;
    private MigrationStopIndicator migrationStopIndicator;
    private ProgressMonitor progressMonitor;
    private ProcessMonitor processMonitor;
    private StatusMonitor statusMonitor;
    private MigrationAliveMonitor migrationAliveMonitor;
    private AbstractMigrationJob migrationJob;
    private final MigrationExitNotifier exitNotifier = new MigrationExitNotifier();

    private MigrationManager() {
        MigrationContext migrationContext = MigrationContext.getInstance();
        this.migrationJobConfig = migrationContext.getMigrationJobConfig();
        this.migrationStopIndicator = migrationContext.getMigrationStopIndicator();
        this.progressMonitor = migrationContext.getProgressMonitor();
        this.processMonitor = migrationContext.getProcessMonitor();
        this.statusMonitor = migrationContext.getStatusMonitor();
        this.migrationAliveMonitor = migrationContext.getMigrationAliveMonitor();
        this.migrationJob = migrationContext.getMigrationJob();
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
                    instance = new MigrationManager();
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
            exitNotifier.fireExit();
            return;
        }

        startMonitor();
        migrationJob.beforeTask();
        migrationJob.startTask(migrationStopIndicator, processMonitor, statusMonitor);

        if (!migrationJobConfig.hasIncrementalMigration() && !migrationJobConfig.hasReverseMigration()) {
            exitNotifier.fireExit();
        }
    }

    /**
     * Register exit callback
     *
     * @param callback exit callback
     */
    public void registerExitCallback(Runnable callback) {
        exitNotifier.registerCallback(callback);
    }

    /**
     * Fire exit callbacks to notify host that migration has exited
     */
    public void fireExit() {
        exitNotifier.fireExit();
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
}
