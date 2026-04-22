/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks;

/**
 * migration task
 *
 * @since 2025/2/28
 */
public interface MigrationTask {
    /**
     * Do something before task
     */
    void beforeTask();

    /**
     * Start task
     */
    void startTask();

    /**
     * Stop task
     */
    void stopTask();

    /**
     * Do something after task
     */
    void afterTask();
}
