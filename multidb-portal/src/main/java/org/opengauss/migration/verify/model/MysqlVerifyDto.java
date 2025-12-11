/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.enums.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MysqlVerifyDto
 *
 * @since 2025/10/31
 */
@Getter
@Setter
public class MysqlVerifyDto extends AbstractVerifyDto {
    private static final Logger LOGGER = LogManager.getLogger(MysqlVerifyDto.class);

    private final MysqlMigrationConfigDto migrationConfigDto;

    private Connection mysqlConnection;
    private Connection opengaussConnection;

    public MysqlVerifyDto(MysqlMigrationConfigDto migrationConfigDto) {
        this.migrationConfigDto = migrationConfigDto;
    }

    public Connection getMysqlConnection() {
        if (mysqlConnection == null) {
            throw new IllegalStateException("MySQL Connect Verify has not been performed");
        }
        return mysqlConnection;
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
        return DatabaseType.MYSQL;
    }

    @Override
    public void closeConnection() {
        try {
            if (mysqlConnection != null) {
                mysqlConnection.close();
            }
            if (opengaussConnection != null) {
                opengaussConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.trace("Failed to close connection", e);
        }
    }
}
