/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import org.opengauss.domain.migration.config.AbstractMigrationConfigDto;
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
    public abstract AbstractMigrationConfigDto getMigrationConfigDto();

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
