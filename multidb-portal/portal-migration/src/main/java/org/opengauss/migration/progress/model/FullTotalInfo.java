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
 * full total info
 *
 * @since 2025/6/3
 */
@Data
public class FullTotalInfo {
    /**
     * all tables total record number, estimated value
     */
    private long record;

    /**
     * all tables total data size, estimated value
     */
    private String data;

    /**
     * migration total time, unit: seconds
     */
    private int time;

    /**
     * migration speed, unit: MB/s
     */
    private String speed;

    /**
     * extract stage speed, unit: rows/s
     */
    private long extractSpeed;

    /**
     * csv write stage speed, unit: rows/s
     */
    private long csvSpeed;

    /**
     * load stage speed, unit: rows/s
     */
    private long loadSpeed;
}
