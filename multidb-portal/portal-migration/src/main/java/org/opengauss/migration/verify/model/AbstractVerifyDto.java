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

package org.opengauss.migration.verify.model;

import org.opengauss.migration.domain.config.AbstractMigrationConfig;
import org.opengauss.enums.DatabaseType;

import java.sql.Connection;

/**
 * AbstractVerifyDto
 *
 * @since 2025/10/31
 */
public abstract class AbstractVerifyDto {
    /**
     * Get migration config dto
     *
     * @return migration config dto
     */
    public abstract AbstractMigrationConfig getMigrationConfigDto();

    /**
     * Get opengauss connection
     *
     * @return opengauss connection
     */
    public abstract Connection getOpengaussConnection();

    /**
     * Set opengauss connection
     *
     * @param opengaussConnection opengauss connection
     */
    public abstract void setOpengaussConnection(Connection opengaussConnection);

    /**
     * Get source db type
     *
     * @return source db type
     */
    public abstract DatabaseType getSourceDbType();

    /**
     * Close connection
     */
    public abstract void closeConnection();
}
