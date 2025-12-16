/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.executor.TaskAssistantExecutor;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tasks.impl.DebeziumPgsqlIncrementalMigrationTask;
import org.opengauss.migration.tasks.impl.DebeziumPgsqlReverseMigrationTask;
import org.opengauss.migration.tasks.impl.FullReplicatePgsqlFullMigrationTask;
import org.opengauss.migration.verify.VerifyManager;

import java.sql.SQLException;

/**
 * PostgreSQL migration job
 *
 * @since 2025/7/3
 */
public class PgsqlMigrationJob extends AbstractMigrationJob {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlMigrationJob.class);

    private final PgsqlMigrationJobConfig migrationJobConfig;

    private boolean hasDoBeforeReverse = false;
    private boolean hasAdjustKernelParam = false;

    public PgsqlMigrationJob(PgsqlMigrationJobConfig migrationJobConfig) {
        this.migrationJobConfig = migrationJobConfig;
        this.hasFullMigration = migrationJobConfig.hasFullMigration();
        this.hasIncrementalMigration = migrationJobConfig.hasIncrementalMigration();
        this.hasReverseMigration = migrationJobConfig.hasReverseMigration();
    }

    @Override
    public boolean preMigrationVerify() {
        return VerifyManager.pgsqlMigrationVerify(migrationJobConfig.getMigrationPhaseList(),
                migrationJobConfig.getMigrationConfigDto(), migrationJobConfig.getTaskWorkspace());
    }

    @Override
    public void beforeTask() {
        try {
            adjustKernelFsyncParam(false, migrationJobConfig.getMigrationConfigDto());
            hasAdjustKernelParam = true;
        } catch (SQLException e) {
            throw new MigrationException("Adjust kernel parameter fsync failed", e);
        }
    }

    @Override
    public void startTask(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor,
                          StatusMonitor statusMonitor) {
        TaskHelper.changePhasesConfig(migrationJobConfig);
        generateTasks(migrationStopIndicator, processMonitor);
        TaskAssistantExecutor executor = getTaskExecutor(migrationStopIndicator, statusMonitor);
        executor.execute();
    }

    @Override
    public synchronized void stopIncremental(
            MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        if (!hasIncrementalMigration) {
            LOGGER.warn("No incremental migration phase, unable to stop incremental migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (!MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING.equals(currentStatus)
                && !MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED.equals(currentStatus)) {
            LOGGER.warn("Can not stop incremental migration, incremental migration is not running or interrupted");
            return;
        }

        statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_STOPPING);
        incrementalMigrationTask.stopTask();

        if (hasFullMigration && !fullMigrationTask.isForeignKeyMigrated()) {
            LOGGER.info("Migrate foreign key");
            fullMigrationTask.migrateForeignKey();
        }

        if (!migrationStopIndicator.isStopped() && hasReverseMigration && !hasDoBeforeReverse) {
            reverseMigrationTask.beforeTask();
            hasDoBeforeReverse = true;
        }
        statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED);
        LOGGER.info("Stop incremental migration successfully");
    }

    @Override
    public synchronized void resumeIncremental(StatusMonitor statusMonitor) {
        if (!hasIncrementalMigration) {
            LOGGER.warn("No incremental migration phase, unable to resume incremental migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (!MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED.equals(currentStatus)) {
            LOGGER.warn("Can not resume incremental migration, incremental migration is not interrupted");
            return;
        }

        incrementalMigrationTask.resumeTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING);
        LOGGER.info("Resume incremental migration successfully");
    }

    @Override
    public synchronized void restartIncremental(
            MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        if (!hasIncrementalMigration) {
            LOGGER.warn("No incremental migration phase, unable to restart incremental migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED.equals(currentStatus)) {
            if (!migrationStopIndicator.isStopped()) {
                clearBeforeReverse();

                statusMonitor.setCurrentStatus(MigrationStatusEnum.START_INCREMENTAL_MIGRATION);
                incrementalMigrationTask.startSource();
                incrementalMigrationTask.startSink();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING);
            }
        } else if (MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED.equals(currentStatus)
                || MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING.equals(currentStatus)) {
            if (!migrationStopIndicator.isStopped()) {
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_STOPPING);
                incrementalMigrationTask.stopTask();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED);

                clearBeforeReverse();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.START_INCREMENTAL_MIGRATION);
                incrementalMigrationTask.startSource();
                incrementalMigrationTask.startSink();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING);
            }
        } else {
            LOGGER.warn("Can not restart incremental migration,"
                    + " incremental migration is not finished or interrupted or running");
            return;
        }
        LOGGER.info("Restart incremental migration successfully");
    }

    @Override
    public synchronized void startReverse(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        if (!hasReverseMigration) {
            LOGGER.warn("No reverse migration phase, unable to start reverse migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (MigrationStatusEnum.START_REVERSE_MIGRATION.equals(currentStatus)
                || MigrationStatusEnum.REVERSE_MIGRATION_RUNNING.equals(currentStatus)
                || MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED.equals(currentStatus)
                || MigrationStatusEnum.REVERSE_MIGRATION_STOPPING.equals(currentStatus)
                || MigrationStatusEnum.REVERSE_MIGRATION_FINISHED.equals(currentStatus)) {
            LOGGER.warn("Reverse migration is already running or interrupted or finished, "
                    + "unable to start reverse migration again");
            return;
        }

        if (!isPreReversePhaseFinished(statusMonitor)) {
            LOGGER.warn("Can not start reverse migration, the previous phase task is not completed");
            return;
        }

        if (hasIncrementalMigration) {
            incrementalMigrationTask.afterTask();
        }

        if (migrationStopIndicator.isStopped()) {
            return;
        }

        if (VerifyManager.pgsqlReversePhaseVerify(migrationJobConfig.getMigrationConfigDto(),
                migrationJobConfig.getTaskWorkspace())) {
            if (!hasDoBeforeReverse) {
                reverseMigrationTask.beforeTask();
            }
            executeReverseTask(statusMonitor);
            LOGGER.info("Start reverse migration successfully");
        } else {
            statusMonitor.setCurrentStatus(MigrationStatusEnum.PRE_REVERSE_PHASE_VERIFY_FAILED);
            LOGGER.info("Reverse migration verify failed, skip reverse migration");
        }
    }

    @Override
    public synchronized void stopReverse(StatusMonitor statusMonitor) {
        if (!hasReverseMigration) {
            LOGGER.warn("No reverse migration phase, unable to stop reverse migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (!MigrationStatusEnum.REVERSE_MIGRATION_RUNNING.equals(currentStatus)
                && !MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED.equals(currentStatus)) {
            LOGGER.warn("Can not stop reverse migration, reverse migration is not running or interrupted");
            return;
        }

        statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_STOPPING);
        reverseMigrationTask.stopTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_FINISHED);
        LOGGER.info("Stop reverse migration successfully");
    }

    @Override
    public synchronized void resumeReverse(StatusMonitor statusMonitor) {
        if (!hasReverseMigration) {
            LOGGER.warn("No reverse migration phase, unable to resume reverse migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (!MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED.equals(currentStatus)) {
            LOGGER.warn("Can not resume reverse migration, reverse migration is not interrupted");
            return;
        }

        reverseMigrationTask.resumeTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_RUNNING);
        LOGGER.info("Resume reverse migration successfully");
    }

    @Override
    public synchronized void restartReverse(
            MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        if (!hasReverseMigration) {
            LOGGER.warn("No reverse migration phase, unable to restart reverse migration");
            return;
        }

        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (MigrationStatusEnum.REVERSE_MIGRATION_FINISHED.equals(currentStatus)) {
            if (!migrationStopIndicator.isStopped()) {
                executeReverseTask(statusMonitor);
            }
        } else if (MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED.equals(currentStatus)
                || MigrationStatusEnum.REVERSE_MIGRATION_RUNNING.equals(currentStatus)) {
            if (!migrationStopIndicator.isStopped()) {
                statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_STOPPING);
                reverseMigrationTask.stopTask();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_FINISHED);

                executeReverseTask(statusMonitor);
            }
        } else {
            LOGGER.warn("Can not restart reverse migration,"
                    + " reverse migration is not finished or interrupted or running");
            return;
        }
        LOGGER.info("Restart reverse migration successfully");
    }

    @Override
    public synchronized void stopTask() {
        if (hasFullMigration) {
            fullMigrationTask.stopTask();
        }

        if (hasIncrementalMigration) {
            incrementalMigrationTask.stopTask();

            if (hasFullMigration && !fullMigrationTask.isForeignKeyMigrated()) {
                LOGGER.info("Migrate foreign key");
                fullMigrationTask.migrateForeignKey();
            }
        }

        if (hasReverseMigration) {
            reverseMigrationTask.stopTask();
        }

        afterTask();
    }

    @Override
    void generateTasks(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor) {
        TaskWorkspace taskWorkspace = migrationJobConfig.getTaskWorkspace();
        PgsqlMigrationConfigDto migrationConfigDto = migrationJobConfig.getMigrationConfigDto();
        if (hasFullMigration) {
            fullMigrationTask = new FullReplicatePgsqlFullMigrationTask(taskWorkspace, migrationStopIndicator,
                    migrationConfigDto, migrationJobConfig.getFullConfigBundle());
        }

        if (hasIncrementalMigration) {
            incrementalMigrationTask = new DebeziumPgsqlIncrementalMigrationTask(processMonitor, migrationStopIndicator,
                    taskWorkspace, migrationConfigDto, migrationJobConfig.getIncrementalConfigBundle());
        }

        if (hasReverseMigration) {
            reverseMigrationTask = new DebeziumPgsqlReverseMigrationTask(processMonitor, migrationStopIndicator,
                    taskWorkspace, migrationConfigDto, migrationJobConfig.getReverseConfigBundle());
        }
    }

    @Override
    void afterTask() {
        if (hasAdjustKernelParam) {
            try {
                adjustKernelFsyncParam(true, migrationJobConfig.getMigrationConfigDto());
            } catch (SQLException e) {
                LOGGER.error("Adjust kernel parameter fsync failed, please manually restore it to on", e);
            }
        }

        if (hasFullMigration) {
            fullMigrationTask.afterTask();
        }

        if (hasIncrementalMigration) {
            incrementalMigrationTask.afterTask();
        }

        if (hasReverseMigration) {
            reverseMigrationTask.afterTask();
        }
    }

    private TaskAssistantExecutor getTaskExecutor(
            MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        TaskAssistantExecutor executor = new TaskAssistantExecutor(migrationStopIndicator);
        if (hasFullMigration) {
            executor.addStep(() -> {
                statusMonitor.setCurrentStatus(MigrationStatusEnum.START_FULL_MIGRATION);
                fullMigrationTask.beforeTask();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_MIGRATION_RUNNING);
                fullMigrationTask.migrateTable();
            });
        }

        if (hasIncrementalMigration) {
            executor.addStep(() -> {
                incrementalMigrationTask.beforeTask();
                incrementalMigrationTask.startSource();
            });
        }

        if (hasFullMigration) {
            if ("true".equals(migrationJobConfig.getMigrationConfigDto().getIsMigrationObject())) {
                executor.addStep(() -> fullMigrationTask.migrateObject());
            } else {
                executor.addStep(() -> {
                    if (!(fullMigrationTask instanceof FullReplicatePgsqlFullMigrationTask)) {
                        throw new IllegalArgumentException("Full migration task is not instance of "
                                + "FullMigrationToolPgsqlFullMigrationTask");
                    }

                    FullReplicatePgsqlFullMigrationTask fullMigrationToolTask =
                            (FullReplicatePgsqlFullMigrationTask) fullMigrationTask;
                    fullMigrationToolTask.waitTableMigrationExit();
                });
            }

            if (!hasIncrementalMigration) {
                executor.addStep(() -> {
                    fullMigrationTask.migrateForeignKey();
                });
            }
            executor.addStep(() -> statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_MIGRATION_FINISHED));
        }
        addIncrementalAndReversePhase(executor, statusMonitor);
        return executor;
    }

    private void addIncrementalAndReversePhase(TaskAssistantExecutor executor, StatusMonitor statusMonitor) {
        if (hasIncrementalMigration) {
            executor.addStep(() -> {
                statusMonitor.setCurrentStatus(MigrationStatusEnum.START_INCREMENTAL_MIGRATION);
                incrementalMigrationTask.startSource();
                incrementalMigrationTask.startSink();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING);
            });
        }

        if (!hasFullMigration && !hasIncrementalMigration && hasReverseMigration) {
            executor.addStep(() -> {
                reverseMigrationTask.beforeTask();
                executeReverseTask(statusMonitor);
            });
        }
    }

    private void executeReverseTask(StatusMonitor statusMonitor) {
        statusMonitor.setCurrentStatus(MigrationStatusEnum.START_REVERSE_MIGRATION);
        reverseMigrationTask.startSource();
        reverseMigrationTask.startSink();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_RUNNING);
    }

    private void clearBeforeReverse() {
        if (hasDoBeforeReverse) {
            reverseMigrationTask.afterTask();
            hasDoBeforeReverse = false;
        }
    }

    private boolean isPreReversePhaseFinished(StatusMonitor statusMonitor) {
        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (hasIncrementalMigration) {
            return MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED.equals(currentStatus);
        }

        if (hasFullMigration) {
            return MigrationStatusEnum.FULL_MIGRATION_FINISHED.equals(currentStatus);
        }
        return true;
    }
}
