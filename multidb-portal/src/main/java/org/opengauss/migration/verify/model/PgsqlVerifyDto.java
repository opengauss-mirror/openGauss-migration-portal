/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.enums.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * PgsqlVerifyDto
 *
 * @since 2025/10/31
 */
@Getter
@Setter
public class PgsqlVerifyDto extends AbstractVerifyDto {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlVerifyDto.class);

    private final PgsqlMigrationConfigDto migrationConfigDto;

    private Connection pgsqlConnection;
    private Connection opengaussConnection;

    public PgsqlVerifyDto(PgsqlMigrationConfigDto migrationConfigDto) {
        this.migrationConfigDto = migrationConfigDto;
    }

    public Connection getPgsqlConnection() {
        if (pgsqlConnection == null) {
            throw new IllegalStateException("PostgreSQL Connect Verify has not been performed");
        }
        return pgsqlConnection;
    }

    @Override
    public Connection getOpengaussConnection() {
        if (opengaussConnection == null) {
            throw new IllegalStateException("OpenGauss Connect Verify has not been performed");
        }
        return opengaussConnection;
    }

    @Override
    public DatabaseType getSourceDbType() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    public void closeConnection() {
        try {
            if (pgsqlConnection != null) {
                pgsqlConnection.close();
            }
            if (opengaussConnection != null) {
                opengaussConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.trace("Failed to close connection", e);
        }
    }
}
