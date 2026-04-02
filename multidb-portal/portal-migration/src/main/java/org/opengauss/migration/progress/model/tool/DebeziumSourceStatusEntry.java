/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model.tool;

import lombok.Data;

/**
 * debezium source status entry
 *
 * @since 2025/6/5
 */
@Data
public class DebeziumSourceStatusEntry {
    private Long timestamp;
    private Integer createCount;
    private Integer skippedExcludeCount;
    private Integer convertCount;
    private Integer pollCount;
    private Integer speed;
    private Integer rest;
}
