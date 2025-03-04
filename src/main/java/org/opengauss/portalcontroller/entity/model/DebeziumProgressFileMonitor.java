/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.portalcontroller.entity.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Monitors progress files of incremental and reverse migration sink/source processes.
 *
 * @since 2025/2/20
 */
@Data
@AllArgsConstructor
public class DebeziumProgressFileMonitor {
    private String processName;
    private String fileHomeParam;
    private String filePrefix;
    private long latestModifiedTimestamp;
    private int repeatedTimes;
}
