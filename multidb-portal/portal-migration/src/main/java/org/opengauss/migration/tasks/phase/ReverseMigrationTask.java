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

package org.opengauss.migration.tasks.phase;

import org.opengauss.migration.tasks.MigrationTask;

/**
 * reverse migration task interface
 *
 * @since 2025/3/20
 */
public interface ReverseMigrationTask extends MigrationTask {
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
