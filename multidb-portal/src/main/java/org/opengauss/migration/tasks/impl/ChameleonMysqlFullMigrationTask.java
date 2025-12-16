/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.opengauss.domain.tool.config.SingleConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.tasks.phase.FullMigrationTask;
import org.opengauss.migration.tasks.tool.ChameleonTask;

/**
 * chameleon mysql full migration task
 *
 * @since 2025/3/20
 */
public class ChameleonMysqlFullMigrationTask extends ChameleonTask implements FullMigrationTask {
    private boolean isTableMigrated = false;
    private boolean isTriggerMigrated = false;
    private boolean isViewMigrated = false;
    private boolean isFunctionMigrated = false;
    private boolean isProcedureMigrated = false;
    private boolean isForeignKeyMigrated = false;

    public ChameleonMysqlFullMigrationTask(TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
                                           SingleConfigBundle chameleonConfig) {
        super(taskWorkspace, migrationStopIndicator, chameleonConfig);
    }

    @Override
    public void beforeTask() {
        super.prepareMigration();
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
        super.triggerMigration();
        isTriggerMigrated = true;

        super.viewMigration();
        isViewMigrated = true;

        super.functionMigration();
        isFunctionMigrated = true;

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
}
