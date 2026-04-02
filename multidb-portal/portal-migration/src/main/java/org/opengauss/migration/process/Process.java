/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process;

/**
 * process interface
 *
 * @since 2025/5/12
 */
public interface Process {
    /**
     * Get process name
     *
     * @return process name
     */
    String getProcessName();

    /**
     * Start process
     */
    void start();

    /**
     * Stop process
     */
    void stop();

    /**
     * Check process status
     *
     * @return whether process is normally
     */
    boolean checkStatus();

    /**
     * Is process alive
     *
     * @return whether process is alive
     */
    boolean isAlive();
}
