/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
    private int record;

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
}
