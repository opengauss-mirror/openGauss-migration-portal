/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.MilvusMigrationJobConfig;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tasks.impl.MilvusMigrationToolFullMigrationTask;
import org.opengauss.migration.verify.VerifyManager;

import java.sql.SQLException;
import java.util.Map;

/**
 * MilvusMigrationJob
 *
 * @since 2025/10/24
 */
public class MilvusMigrationJob extends AbstractMigrationJob {
    private static final Logger LOGGER = LogManager.getLogger(MilvusMigrationJob.class);

    private final MilvusMigrationJobConfig migrationJobConfig;

    private boolean hasAdjustKernelParam = false;

    public MilvusMigrationJob(MilvusMigrationJobConfig migrationJobConfig) {
        this.migrationJobConfig = migrationJobConfig;
        this.hasFullMigration = true;
    }

    @Override
    public boolean preMigrationVerify() {
        return VerifyManager.milvusMigrationVerify(
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
        processMonitor.stopMonitoring();
        TaskHelper.changePhasesConfig(migrationJobConfig);
        generateTasks(migrationStopIndicator, processMonitor);

        statusMonitor.setCurrentStatus(MigrationStatusEnum.START_FULL_MIGRATION);
        fullMigrationTask.beforeTask();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_MIGRATION_RUNNING);
        fullMigrationTask.migrateTable();
        statusMonitor.setCurrentStatus(MigrationStatusEnum.FULL_MIGRATION_FINISHED);
    }

    @Override
    public void stopIncremental(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        LOGGER.error("Stop incremental migration method is not supported");
    }

    @Override
    public void resumeIncremental(StatusMonitor statusMonitor) {
        LOGGER.error("Resume incremental migration method is not supported");
    }

    @Override
    public void restartIncremental(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        LOGGER.error("Restart incremental migration method is not supported");
    }

    @Override
    public void startReverse(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        LOGGER.error("Start reverse migration method is not supported");
    }

    @Override
    public void stopReverse(StatusMonitor statusMonitor) {
        LOGGER.error("Stop reverse migration method is not supported");
    }

    @Override
    public void resumeReverse(StatusMonitor statusMonitor) {
        LOGGER.error("Resume reverse migration method is not supported");
    }

    @Override
    public void restartReverse(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor) {
        LOGGER.error("Restart reverse migration method is not supported");
    }

    @Override
    public void stopTask() {
        fullMigrationTask.stopTask();
        afterTask();
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

        fullMigrationTask.afterTask();
    }

    @Override
    void generateTasks(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor) {
        TaskWorkspace taskWorkspace = migrationJobConfig.getTaskWorkspace();
        MilvusMigrationConfigDto migrationConfigDto = migrationJobConfig.getMigrationConfigDto();
        Map<String, String> tableMappings = migrationJobConfig.getTableMappings();
        fullMigrationTask = new MilvusMigrationToolFullMigrationTask(
                taskWorkspace, migrationConfigDto, migrationJobConfig.getFullConfigBundle().getConfigFile(),
                tableMappings, Integer.parseInt(migrationConfigDto.getMigrationConcurrentThreads()));
    }
}
