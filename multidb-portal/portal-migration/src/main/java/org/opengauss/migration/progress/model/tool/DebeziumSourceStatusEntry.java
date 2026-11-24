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
