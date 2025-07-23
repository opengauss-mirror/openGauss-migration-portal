/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * task dto
 *
 * @since 2025/4/24
 */
@Data
@NoArgsConstructor
public class TaskListVo {
    private String taskId;
    private String sourceDbType;

    /**
     * task is running or not
     */
    private boolean isRunning;
}
