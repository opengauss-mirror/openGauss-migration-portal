/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
