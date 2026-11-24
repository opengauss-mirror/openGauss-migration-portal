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
