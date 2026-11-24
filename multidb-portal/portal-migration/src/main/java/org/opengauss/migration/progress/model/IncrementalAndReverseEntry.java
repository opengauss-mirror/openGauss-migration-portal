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
