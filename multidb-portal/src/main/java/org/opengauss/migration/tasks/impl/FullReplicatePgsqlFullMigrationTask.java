/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.FullReplicateConfig;
import org.opengauss.domain.dto.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.FullReplicateConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.config.DebeziumPgsqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.FullReplicatePgsqlMigrationConfigHelper;
import org.opengauss.migration.helper.tool.FullReplicateHelper;
import org.opengauss.migration.tasks.phase.FullMigrationTask;
import org.opengauss.migration.tasks.tool.FullReplicateTask;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.HashMap;

/**
 * oG_datasync_full_migration pgsql full migration task
 *
 * @since 2025/5/29
 */
public class FullReplicatePgsqlFullMigrationTask extends FullReplicateTask implements FullMigrationTask {
    private static final Logger LOGGER = LogManager.getLogger(FullReplicatePgsqlFullMigrationTask.class);

    private final PgsqlMigrationConfigDto migrationConfigDto;
    private boolean isTableMigrated = false;
    private boolean isTriggerMigrated = false;
    private boolean isViewMigrated = false;
    private boolean isFunctionMigrated = false;
    private boolean isProcedureMigrated = false;
    private boolean isForeignKeyMigrated = false;

    public FullReplicatePgsqlFullMigrationTask(
            TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
            PgsqlMigrationConfigDto migrationConfigDto, FullReplicateConfigBundle fullMigrationToolConfig) {
        super(taskWorkspace, migrationStopIndicator, fullMigrationToolConfig, migrationConfigDto.getFullProcessJvm());
        this.migrationConfigDto = migrationConfigDto;
    }

    @Override
    public void beforeTask() {
        super.dropReplicaSchema();
        cleanHistoryFiles();
        setSlotName();
    }

    @Override
    public void migrateTable() {
        super.tableMigration();
    }

    @Override
    public void waitTableMigrationExit() {
        super.waitTableMigrationExit();
        isTableMigrated = true;
    }

    @Override
    public void migrateObject() {
        waitTableMigrationExit();

        super.viewMigration();
        isViewMigrated = true;

        super.functionMigration();
        isFunctionMigrated = true;

        super.triggerMigration();
        isTriggerMigrated = true;

        super.procedureMigration();
        isProcedureMigrated = true;
    }

    @Override
    public void migrateForeignKey() {
        super.foreignKeyMigration();
        isForeignKeyMigrated = true;
    }

    @Override
    public boolean isTableMigrated() {
        return isTableMigrated;
    }

    @Override
    public boolean isTriggerMigrated() {
        return isTriggerMigrated;
    }

    @Override
    public boolean isViewMigrated() {
        return isViewMigrated;
    }

    @Override
    public boolean isFunctionMigrated() {
        return isFunctionMigrated;
    }

    @Override
    public boolean isProcedureMigrated() {
        return isProcedureMigrated;
    }

    @Override
    public boolean isForeignKeyMigrated() {
        return isForeignKeyMigrated;
    }

    @Override
    public void stopTask() {
        super.stop();
    }

    @Override
    public void afterTask() {
        super.afterMigration();
    }

    private void cleanHistoryFiles() {
        String csvDirPath = FullReplicatePgsqlMigrationConfigHelper.generateCsvDirPath(taskWorkspace);
        String logPath = FullReplicateHelper.generateFullMigrationLogPath(taskWorkspace);
        String statusDirPath = taskWorkspace.getStatusFullDirPath();
        try {
            FileUtils.deletePath(csvDirPath);
            FileUtils.deletePath(logPath);
            FileUtils.cleanDirectory(statusDirPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to delete oG_datasync_full_migration history files, error: {}", e.getMessage());
        }
    }

    private void setSlotName() {
        String slotName = DebeziumPgsqlMigrationConfigHelper.generateIncrementalSlotName(migrationConfigDto,
                taskWorkspace);
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(FullReplicateConfig.SLOT_NAME, slotName);
        fullConfig.changeConfig(changeParams);
    }
}
