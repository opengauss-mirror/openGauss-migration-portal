/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.opengauss.domain.migration.config.ElasticsearchMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.tasks.phase.FullMigrationTask;
import org.opengauss.migration.tasks.tool.ElasticsearchMigrationToolTask;

import java.util.Map;

/**
 * ElasticsearchMigrationToolFullMigrationTask
 *
 * @since 2025/10/27
 */
public class ElasticsearchMigrationToolFullMigrationTask
        extends ElasticsearchMigrationToolTask implements FullMigrationTask {
    private boolean isTableMigrated = false;

    public ElasticsearchMigrationToolFullMigrationTask(
            TaskWorkspace taskWorkspace, ElasticsearchMigrationConfigDto migrationConfigDto, ConfigFile configFile,
            Map<String, String> tableMappings, int migrationConcurrentThreads
    ) {
        super(taskWorkspace, migrationConfigDto, configFile, tableMappings, migrationConcurrentThreads);
    }

    @Override
    public void beforeTask() {
        super.prepareMigration();
    }

    @Override
    public void stopTask() {
        super.stopMigration();
    }

    @Override
    public void afterTask() {
        super.afterMigration();
    }

    @Override
    public void migrateTable() {
        super.startMigration();
        isTableMigrated = true;
    }

    @Override
    public void migrateObject() {
        throw new UnsupportedOperationException("Migrate object method is not supported");
    }

    @Override
    public void migrateForeignKey() {
        throw new UnsupportedOperationException("Migrate foreign key method is not supported");
    }

    @Override
    public boolean isTableMigrated() {
        return isTableMigrated;
    }

    @Override
    public boolean isTriggerMigrated() {
        return false;
    }

    @Override
    public boolean isViewMigrated() {
        return false;
    }

    @Override
    public boolean isFunctionMigrated() {
        return false;
    }

    @Override
    public boolean isProcedureMigrated() {
        return false;
    }

    @Override
    public boolean isForeignKeyMigrated() {
        return false;
    }
}
