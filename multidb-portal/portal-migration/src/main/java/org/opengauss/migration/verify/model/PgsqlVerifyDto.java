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

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.domain.config.PgsqlMigrationConfig;
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

    private final PgsqlMigrationConfig migrationConfigDto;

    private Connection pgsqlConnection;
    private Connection opengaussConnection;

    public PgsqlVerifyDto(PgsqlMigrationConfig migrationConfigDto) {
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
