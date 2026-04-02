/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.phase;

import org.opengauss.migration.tasks.MigrationTask;

/**
 * incremental data check task interface
 *
 * @since 2025/3/20
 */
public interface IncrementalMigrationTask extends MigrationTask {
    @Override
    default void startTask() {
        startSink();
        startSource();
    }

    /**
     * Start source process
     */
    void startSource();

    /**
     * Start sink process
     */
    void startSink();

    /**
     * Stop source process
     */
    void stopSource();

    /**
     * Stop sink process
     */
    void stopSink();

    /**
     * Resume pause task
     */
    void resumeTask();

    @Override
    default void stopTask() {
        stopSource();
        stopSink();
    }
}
