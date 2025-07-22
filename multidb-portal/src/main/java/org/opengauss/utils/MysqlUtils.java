/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.opengauss.constants.SqlConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Mysql utils
 *
 * @since 2025/7/8
 */
public class MysqlUtils {
    /**
     * Get the value of a variable
     *
     * @param variableName variable name
     * @param connection   connection
     * @return variable value
     * @throws SQLException if a database access error occurs
     */
    public static String getVariableValue(String variableName, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.MYSQL_SHOW_VARIABLE)) {
            statement.setString(1, variableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(2);
                }
            }
        }
        throw new SQLException("Variable " + variableName + " not found");
    }

    /**
     * Check if a user has a permission
     *
     * @param permission permission
     * @param username   username
     * @param connection connection
     * @return true if the user has the permission, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean hasPermission(String permission, String username, Connection connection) throws SQLException {
        if (!permission.matches("[a-zA-Z0-9_]+_priv")) {
            throw new IllegalArgumentException("Invalid permission name");
        }

        String sql = String.format(SqlConstants.MYSQL_SELECT_USER_COLUMN, permission, username);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return "Y".equals(resultSet.getString(1));
            }
        }
        throw new SQLException("User " + username + " does not exists");
    }

    /**
     * Get the value of Executed_Gtid_Set
     *
     * @param connection connection
     * @return Executed_Gtid_Set
     * @throws SQLException if a database access error occurs
     */
    public static String getExecutedGtidSet(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SqlConstants.MYSQL_SHOW_MASTER_STATUS)) {
            if (resultSet.next()) {
                return resultSet.getString("Executed_Gtid_Set");
            }
        }
        throw new SQLException("Failed to execute SQL to get Executed_Gtid_Set");
    }
}
