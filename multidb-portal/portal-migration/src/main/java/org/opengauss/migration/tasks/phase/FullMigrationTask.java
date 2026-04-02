/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.phase;

import org.opengauss.migration.tasks.MigrationTask;

/**
 * full migration task interface
 *
 * @since 2025/3/20
 */
public interface FullMigrationTask extends MigrationTask {
    @Override
    default void startTask() {
        throw new UnsupportedOperationException("Start task method is not supported");
    }

    /**
     * Migrate table
     */
    void migrateTable();

    /**
     * Migrate object
     */
    void migrateObject();

    /**
     * Migrate foreign key
     */
    void migrateForeignKey();

    /**
     * Is table migrated
     *
     * @return true if table migrated, otherwise false
     */
    boolean isTableMigrated();

    /**
     * Is trigger migrated
     *
     * @return true if trigger migrated, otherwise false
     */
    boolean isTriggerMigrated();

    /**
     * Is view migrated
     *
     * @return true if view migrated, otherwise false
     */
    boolean isViewMigrated();

    /**
     * Is function migrated
     *
     * @return true if function migrated, otherwise false
     */
    boolean isFunctionMigrated();

    /**
     * Is procedure migrated
     *
     * @return true if procedure migrated, otherwise false
     */
    boolean isProcedureMigrated();

    /**
     * Is foreign key migrated
     *
     * @return true if foreign key migrated, otherwise false
     */
    boolean isForeignKeyMigrated();
}
