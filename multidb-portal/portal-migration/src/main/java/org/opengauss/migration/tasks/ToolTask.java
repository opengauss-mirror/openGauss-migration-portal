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
