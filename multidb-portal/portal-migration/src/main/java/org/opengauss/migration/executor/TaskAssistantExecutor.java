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

package org.opengauss.migration.executor;

import org.opengauss.migration.domain.model.MigrationStopIndicator;

import java.util.ArrayList;
import java.util.List;

/**
 * Migration task assistant executor
 *
 * @since 2025/3/25
 */
public class TaskAssistantExecutor {
    private final MigrationStopIndicator migrationStopIndicator;
    private final List<Runnable> steps = new ArrayList<>();
    private int currentTaskIndex = 0;

    public TaskAssistantExecutor(MigrationStopIndicator taskControlOrder) {
        this.migrationStopIndicator = taskControlOrder;
    }

    /**
     * Add migration step
     *
     * @param step migration step
     */
    public void addStep(Runnable step) {
        steps.add(step);
    }

    /**
     * Execute migration steps
     */
    public void execute() {
        while (currentTaskIndex < steps.size() && !migrationStopIndicator.isStopped()) {
            steps.get(currentTaskIndex++).run();
        }
    }
}
