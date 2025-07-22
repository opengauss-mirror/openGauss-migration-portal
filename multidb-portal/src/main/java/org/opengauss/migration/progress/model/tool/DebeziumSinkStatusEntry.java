/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model.tool;

import lombok.Data;

/**
 * debezium sink status entry
 *
 * @since 2025/6/5
 */
@Data
public class DebeziumSinkStatusEntry {
    private Long timestamp;
    private Integer extractCount;
    private Integer skippedExcludeEventCount;
    private Integer skippedCount;
    private Integer replayedCount;
    private Integer successCount;
    private Integer failCount;
    private Integer speed;
    private Integer rest;
    private Integer overallPipe;
}
