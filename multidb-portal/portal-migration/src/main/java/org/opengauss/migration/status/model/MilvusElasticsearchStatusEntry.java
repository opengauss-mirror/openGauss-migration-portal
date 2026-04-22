/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.status.model;

import lombok.Data;

/**
 * MilvusElasticsearchStatusEntry
 *
 * @since 2025/11/3
 */
@Data
public class MilvusElasticsearchStatusEntry {
    private String name;

    /**
     * status: 0 - success, 1 - fail
     */
    private int status;

    public MilvusElasticsearchStatusEntry(String name, int status) {
        this.name = name;
        this.status = status;
    }
}
