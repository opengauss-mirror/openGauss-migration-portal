/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.Getter;

/**
 * database type
 *
 * @since 2025/2/27
 */
@Getter
public enum DatabaseType {
    MYSQL("MySQL"),
    OPENGAUSS("openGauss"),
    POSTGRESQL("PostgreSQL"),
    MILVUS("Milvus"),
    ELASTICSEARCH("Elasticsearch"),
    ;

    DatabaseType(String standardName) {
        this.standardName = standardName;
    }

    private final String standardName;
}
