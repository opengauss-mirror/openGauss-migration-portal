/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.executor;

import org.opengauss.domain.model.MigrationStopIndicator;

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
