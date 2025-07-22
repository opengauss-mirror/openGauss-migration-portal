/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.opengauss.migration.MigrationManager;

/**
 * Task Service
 *
 * @since 2025/2/27
 */
@Named("taskService")
@ApplicationScoped
public class TaskService {
    /**
     * Start task
     */
    public void startTask() {
        MigrationManager.getInstance().start();
    }

    /**
     * Stop incremental
     */
    public void stopIncremental() {
        MigrationManager.getInstance().stopIncremental();
    }

    /**
     * Resume incremental
     */
    public void resumeIncremental() {
        MigrationManager.getInstance().resumeIncremental();
    }

    /**
     * Restart incremental
     */
    public void restartIncremental() {
        MigrationManager.getInstance().restartIncremental();
    }

    /**
     * Start reverse
     */
    public void startReverse() {
        MigrationManager.getInstance().startReverse();
    }

    /**
     * Stop reverse
     */
    public void stopReverse() {
        MigrationManager.getInstance().stopReverse();
    }

    /**
     * Resume reverse
     */
    public void resumeReverse() {
        MigrationManager.getInstance().resumeReverse();
    }

    /**
     * Restart reverse
     */
    public void restartReverse() {
        MigrationManager.getInstance().restartReverse();
    }

    /**
     * Stop task
     */
    public void stopTask() {
        MigrationManager.getInstance().stop();
    }
}
