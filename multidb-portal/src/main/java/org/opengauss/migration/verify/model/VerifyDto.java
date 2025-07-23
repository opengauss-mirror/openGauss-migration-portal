/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.VerifyException;
import org.opengauss.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Verify dto
 *
 * @since 2025/5/9
 */
@Data
public class VerifyDto {
    private static final Logger LOGGER = LogManager.getLogger(VerifyDto.class);

    private DatabaseType sourceDbType;

    private String sourceIp;
    private String sourcePort;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDatabase;

    private String targetIp;
    private String targetPort;
    private String targetUsername;
    private String targetPassword;
    private String targetDatabase;

    private boolean isTargetCluster;
    private String targetStandbyHosts;
    private String targetStandbyPorts;

    private Connection sourceConnection;
    private Connection targetConnection;

    /**
     * Close connection
     */
    public void closeConnection() {
        if (sourceConnection != null) {
            try {
                sourceConnection.close();
            } catch (SQLException e) {
                LOGGER.trace("Failed to close source connection", e);
            }
        }

        if (targetConnection != null) {
            try {
                targetConnection.close();
            } catch (SQLException e) {
                LOGGER.trace("Failed to close target connection", e);
            }
        }
    }

    /**
     * Check connection, if connection is null, create connection
     */
    public void checkConnection() {
        try {
            if (sourceConnection == null) {
                if (DatabaseType.MYSQL.equals(sourceDbType)) {
                    sourceConnection = JdbcUtils.getMysqlConnection(
                            sourceIp, sourcePort, sourceDatabase, sourceUsername, sourcePassword);
                } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
                    sourceConnection = JdbcUtils.getPgsqlConnection(
                            sourceIp, sourcePort, sourceDatabase, sourceUsername, sourcePassword);
                } else {
                    throw new VerifyException("Unsupported source database type: " + sourceDbType);
                }
            }

            if (targetConnection == null) {
                targetConnection = JdbcUtils.getOpengaussConnection(
                        targetIp, targetPort, targetDatabase, targetUsername, targetPassword);
            }
        } catch (ClassNotFoundException | SQLException e) {
            throw new VerifyException("Failed to get connection", e);
        }

        if (!testConnection(sourceConnection)) {
            throw new VerifyException(sourceDbType.getStandardName() + " connection is unavailable");
        }
        if (!testConnection(targetConnection)) {
            throw new VerifyException("openGauss connection is unavailable");
        }
    }

    private boolean testConnection(Connection connection) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to test connection", e);
            return false;
        }
    }
}
