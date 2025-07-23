/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model;

import lombok.Data;

/**
 * incremental and reverse entry
 *
 * @since 2025/6/5
 */
@Data
public class IncrementalAndReverseEntry {
    private Integer count;
    private Integer replayedCount;
    private Integer skippedCount;
    private Integer successCount;
    private Integer failCount;
    private Integer rest;
    private Integer sourceSpeed;
    private Integer sinkSpeed;
    private Boolean hasFailSql;
}
