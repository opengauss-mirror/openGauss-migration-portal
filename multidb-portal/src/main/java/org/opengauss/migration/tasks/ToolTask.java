/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks;

import org.opengauss.domain.model.TaskWorkspace;

/**
 * Tool task
 *
 * @since 2025/3/20
 */
public abstract class ToolTask {
    /**
     * Task workspace
     */
    protected final TaskWorkspace taskWorkspace;

    protected ToolTask(TaskWorkspace taskWorkspace) {
        this.taskWorkspace = taskWorkspace;
    }
}
