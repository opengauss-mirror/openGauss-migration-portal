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
