/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model;

import lombok.Data;

/**
 * Check fail entry
 *
 * @since 2025/6/4
 */
@Data
public class CheckFailEntry extends CheckEntry {
    /**
     * error message, default is ""
     */
    private String error;

    /**
     * repair file path, default is ""
     */
    private String repairFilePath;
}
