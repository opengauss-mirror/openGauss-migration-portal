/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.MysqlMigrationJobConfig;
import org.opengauss.migration.executor.TaskAssistantExecutor;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tasks.impl.ChameleonMysqlFullMigrationTask;
import org.opengauss.migration.tasks.impl.DataCheckerMysqlFullDataCheckTask;
import org.opengauss.migration.tasks.impl.DataCheckerMysqlIncrementalDataCheckTask;
import org.opengauss.migration.tasks.impl.DebeziumMysqlIncrementalMigrationTask;
import org.opengauss.migration.tasks.impl.DebeziumMysqlReverseMigrationTask;
import org.opengauss.migration.verify.VerifyManager;

import java.sql.SQLException;

/**
 * Mysql Migration Job
 *
 * @since 2025/7/2
 */
public class MysqlMigrationJob extends AbstractMigrationJob {
    private static final Logger LOGGER = LogManager.getLogger(MysqlMigrationJob.class);

    private final MysqlMigrationJobConfig migrationJobConfig;

    private boolean hasDoBeforeReverse = false;
    private boolean hasAdjustKernelParam = false;

    public MysqlMigrationJob(MysqlMigrationJobConfig migrationJobConfig) {
        this.migrationJobConfig = migrationJobConfig;
        this.hasFullMigration = migrationJobConfig.hasFullMigration();
        this.hasFullDataCheck = migrationJobConfig.hasFullDataCheck();
        this.hasIncrementalMigration = migrationJobConfig.hasIncrementalMigration();
        this.hasIncrementalDataCheck = migrationJobConfig.hasIncrementalDataCheck();
        this.hasReverseMigration = migrationJobConfig.hasReverseMigration();
    }

    @Override
    public boolean preMigrationVerify() {
        return VerifyManager.mysqlMigrationVerify(migrationJobConfig.getMigrationPhaseList(),
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
        if (!currentStatus.equals(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING)
                && !currentStatus.equals(MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED)) {
            LOGGER.warn("Can not stop incremental migration, incremental migration is not running or interrupted");
            return;
        }

        statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_STOPPING);
        if (hasIncrementalDataCheck) {
            incrementalDataCheckTask.stopTask();
            LOGGER.info("Stop incremental data check successfully");
        }
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

        if (!statusMonitor.getCurrentStatus().getStatus().equals(
                MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED)) {
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

                if (hasIncrementalDataCheck) {
                    incrementalDataCheckTask.startTask();
                }
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING);
            }
        } else if (MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED.equals(currentStatus)
                || MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING.equals(currentStatus)) {
            if (!migrationStopIndicator.isStopped()) {
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_STOPPING);
                if (hasIncrementalDataCheck) {
                    incrementalDataCheckTask.stopTask();
                }
                incrementalMigrationTask.stopTask();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED);

                clearBeforeReverse();
                statusMonitor.setCurrentStatus(MigrationStatusEnum.START_INCREMENTAL_MIGRATION);
                incrementalMigrationTask.startSource();
                incrementalMigrationTask.startSink();
                if (hasIncrementalDataCheck) {
                    incrementalDataCheckTask.startTask();
                }
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
            LOGGER.warn("Reverse migration is already running or interrupted or finished"
                    + ", unable to start reverse migration again");
            return;
        }

        if (!isPreReversePhaseFinished(statusMonitor)) {
            LOGGER.warn("Can not start reverse migration, the previous phase task is not completed");
            return;
        }

        if (migrationStopIndicator.isStopped()) {
            return;
        }

        if (VerifyManager.mysqlReversePhaseVerify(migrationJobConfig.getMigrationConfigDto(),
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
        if (hasFullMigration && fullMigrationTask != null) {
            fullMigrationTask.stopTask();
        }
        if (hasFullDataCheck && fullDataCheckTask != null) {
            fullDataCheckTask.stopTask();
        }
        if (hasIncrementalMigration && incrementalMigrationTask != null) {
            if (hasIncrementalDataCheck && incrementalDataCheckTask != null) {
                incrementalDataCheckTask.stopTask();
            }
            incrementalMigrationTask.stopTask();

            if (hasFullMigration && !fullMigrationTask.isForeignKeyMigrated()) {
                LOGGER.info("Migrate foreign key");
                fullMigrationTask.migrateForeignKey();
            }
        }
        if (hasReverseMigration && reverseMigrationTask != null) {
            reverseMigrationTask.stopTask();
        }

        afterTask();
    }

    @Override
    void generateTasks(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor) {
        TaskWorkspace taskWorkspace = migrationJobConfig.getTaskWorkspace();
        MysqlMigrationConfigDto migrationConfigDto = migrationJobConfig.getMigrationConfigDto();
        if (hasFullMigration) {
            fullMigrationTask = new ChameleonMysqlFullMigrationTask(taskWorkspace, migrationStopIndicator,
                    migrationJobConfig.getFullConfigBundle());
        }

        if (hasFullDataCheck) {
            fullDataCheckTask = new DataCheckerMysqlFullDataCheckTask(processMonitor, migrationStopIndicator,
                    taskWorkspace, migrationConfigDto, migrationJobConfig.getFullDataCheckConfigBundle());
        }

        if (hasIncrementalMigration) {
            incrementalMigrationTask = new DebeziumMysqlIncrementalMigrationTask(processMonitor, migrationStopIndicator,
                    taskWorkspace, migrationConfigDto, migrationJobConfig.getIncrementalConfigBundle());

            if (hasIncrementalDataCheck) {
                incrementalDataCheckTask = new DataCheckerMysqlIncrementalDataCheckTask(processMonitor,
                        migrationStopIndicator, taskWorkspace, migrationConfigDto,
                        migrationJobConfig.getIncrementalDataCheckConfigBundle());
            }
        }

        if (hasReverseMigration) {
            reverseMigrationTask = new DebeziumMysqlReverseMigrationTask(processMonitor, migrationStopIndicator,
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

        if (hasFullMigration && fullMigrationTask != null) {
            fullMigrationTask.afterTask();
        }

        if (hasFullDataCheck && fullDataCheckTask != null) {
            fullDataCheckTask.afterTask();
        }

        if (hasIncrementalMigration && incrementalMigrationTask != null) {
            incrementalMigrationTask.afterTask();
            if (hasIncrementalDataCheck && incrementalDataCheckTask != null) {
                incrementalDataCheckTask.afterTask();
            }
        }

        if (hasReverseMigration && reverseMigrationTask != null) {
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
                    if (!(fullMigrationTask instanceof ChameleonMysqlFullMigrationTask)) {
                        throw new IllegalArgumentException("Full migration task is not "
                                + "ChameleonMysqlFullMigrationTask");
                    }
                    ChameleonMysqlFullMigrationTask chameleonTask = (ChameleonMysqlFullMigrationTask) fullMigrationTask;
                    chameleonTask.waitTableMigrationExit();
                });
            }

            if (!hasIncrementalMigration) {
                executor.addStep(() -> {
                    fullMigrationTask.migrateForeignKey();
                });
            }
            executor.addStep(() -> statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_MIGRATION_FINISHED));
        }

        if (hasFullDataCheck) {
            executor.addStep(() -> executeFullDataCheckTask(statusMonitor));
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
            });
            if (hasIncrementalDataCheck) {
                executor.addStep(() -> {
                    incrementalDataCheckTask.beforeTask();
                    incrementalDataCheckTask.startTask();
                });
            }
            executor.addStep(() -> statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING));
        }

        if (!hasFullMigration && !hasFullDataCheck && !hasIncrementalMigration && hasReverseMigration) {
            executor.addStep(() -> {
                reverseMigrationTask.beforeTask();
                executeReverseTask(statusMonitor);
            });
        }
    }

    private boolean isPreReversePhaseFinished(StatusMonitor statusMonitor) {
        MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
        if (hasIncrementalMigration) {
            return MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED.equals(currentStatus);
        }

        if (hasFullDataCheck) {
            return MigrationStatusEnum.FULL_DATA_CHECK_FINISHED.equals(currentStatus);
        }

        if (hasFullMigration) {
            return MigrationStatusEnum.FULL_MIGRATION_FINISHED.equals(currentStatus);
        }
        return true;
    }

    private void executeFullDataCheckTask(StatusMonitor statusMonitor) {
        statusMonitor.setCurrentStatus(MigrationStatusEnum.START_FULL_DATA_CHECK);
        fullDataCheckTask.beforeTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_DATA_CHECK_RUNNING);
        fullDataCheckTask.startTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_DATA_CHECK_FINISHED);
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
}
