/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model;

import lombok.Data;

/**
 * check entry
 *
 * @since 2025/6/4
 */
@Data
public class CheckEntry {
    /**
     * schema name
     */
    protected String schema;

    /**
     * table name
     */
    protected String name;
}
