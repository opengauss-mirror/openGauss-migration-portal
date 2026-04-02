/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import lombok.Data;

/**
 * chain result
 *
 * @since 2025/5/8
 */
@Data
public class ChainResult {
    private String name;
    private boolean isSuccess;
    private String detail;

    public ChainResult() {
        this.isSuccess = true;
    }
}
