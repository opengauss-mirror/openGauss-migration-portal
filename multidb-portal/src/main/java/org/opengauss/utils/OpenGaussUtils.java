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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * openGauss utils
 *
 * @since 2025/5/27
 */
public class OpenGaussUtils {
    private static final Pattern GS_VERSION_PATTERN = Pattern.compile("\\(openGauss\\s(\\d+\\.\\d+\\.\\d+[^\\s]*)");

    /**
     * Check whether the user is a system administrator
     *
     * @param username   username
     * @param connection connection
     * @return true if the user is a system administrator, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean isSystemAdmin(String username, Connection connection) throws SQLException {
        boolean isAdmin = false;
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.OPENGAUSS_IS_SYSTEM_ADMIN)) {
            statement.setString(1, username);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String permissionStr = resultSet.getString("rolsystemadmin");
                    isAdmin = permissionStr.equals("1") || permissionStr.equals("t");
                }
            }
        }
        return isAdmin;
    }

    /**
     * Alter system set
     *
     * @param paramName param name
     * @param value value
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void alterSystemSet(String paramName, String value, Connection connection) throws SQLException {
        String sql = String.format(SqlConstants.OPENGAUSS_ALTER_SYSTEM_SET, paramName, value);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Check whether the user is a replication role
     *
     * @param username   username
     * @param connection connection
     * @return true if the user is a replication role, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean isReplicationRole(String username, Connection connection) throws SQLException {
        boolean isReplicationRole = false;
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.OPENGAUSS_IS_REPLICATION_ROLE)) {
            statement.setString(1, username);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String permissionStr = resultSet.getString("rolreplication");
                    isReplicationRole = permissionStr.equals("1") || permissionStr.equals("t");
                }
            }
        }
        return isReplicationRole;
    }

    /**
     * Get the openGauss version
     *
     * @param connection connection
     * @return openGauss version
     * @throws SQLException if a database access error occurs
     */
    public static String getOpenGaussVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SqlConstants.SELECT_VERSION)) {
            if (resultSet.next()) {
                String rsString = resultSet.getString("version");
                Matcher matcher = GS_VERSION_PATTERN.matcher(rsString);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
        }
        throw new SQLException("Not found OpenGauss version");
    }

    /**
     * Get the database access permissions
     *
     * @param databaseName database name
     * @param connection connection
     * @return database access permissions
     * @throws SQLException if a database access error occurs
     */
    public static String getDatabaseAccessPermissions(String databaseName, Connection connection)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.OPENGAUSS_ACCESS_PERMISSIONS)) {
            statement.setString(1, databaseName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("datacl");
                }
            }
        }
        throw new SQLException("Database " + databaseName + " not found");
    }

    /**
     * Get schema table names
     *
     * @param schema     schema name
     * @param connection pgsql connection
     * @return schema table names
     * @throws SQLException sql exception
     */
    public static List<String> getSchemaTableNames(String schema, Connection connection) throws SQLException {
        return PgsqlUtils.getSchemaTableNames(schema, connection);
    }

    /**
     * Get the value of a variable
     *
     * @param variableName variable name
     * @param connection   connection
     * @return variable value
     * @throws SQLException if a database access error occurs
     */
    public static String getVariableValue(String variableName, Connection connection) throws SQLException {
        return PgsqlUtils.getVariableValue(variableName, connection);
    }

    /**
     * Check whether the schema exists
     *
     * @param schemaName schema name
     * @param connection connection
     * @return true if the schema exists, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean isSchemaExists(String schemaName, Connection connection) throws SQLException {
        return PgsqlUtils.isSchemaExists(schemaName, connection);
    }

    /**
     * Create a replication slot
     *
     * @param slotName slot name
     * @param plugin plugin name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void createReplicationSlot(String slotName, String plugin, Connection connection)
            throws SQLException {
        PgsqlUtils.createReplicationSlot(slotName, plugin, connection);
    }

    /**
     * Drop a replication slot
     *
     * @param slotName slot name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void dropReplicationSlot(String slotName, Connection connection) throws SQLException {
        PgsqlUtils.dropReplicationSlot(slotName, connection);
    }

    /**
     * Get the replication slot names
     *
     * @param connection connection
     * @return replication slot names list
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getReplicationSlotNames(Connection connection) throws SQLException {
        return PgsqlUtils.getReplicationSlotNames(connection);
    }

    /**
     * Get the number of replication slots
     *
     * @param connection connection
     * @return number of replication slots
     * @throws SQLException if a database access error occurs
     */
    public static int countReplicationSlots(Connection connection) throws SQLException {
        return PgsqlUtils.countReplicationSlots(connection);
    }

    /**
     * Get the publication names
     *
     * @param connection connection
     * @return publication names list
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getPublicationNames(Connection connection) throws SQLException {
        return PgsqlUtils.getPublicationNames(connection);
    }

    /**
     * Drop a publication
     *
     * @param publicationName publication name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void dropPublication(String publicationName, Connection connection) throws SQLException {
        PgsqlUtils.dropPublication(publicationName, connection);
    }

    /**
     * Create publication for all tables
     *
     * @param publicationName publication name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void createPublicationAllTables(String publicationName, Connection connection) throws SQLException {
        PgsqlUtils.createPublicationAllTables(publicationName, connection);
    }

    /**
     * Create publication for table list
     *
     * @param publicationName publication name
     * @param tableList table list
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void createPublicationForTable(String publicationName, List<String> tableList, Connection connection)
            throws SQLException {
        PgsqlUtils.createPublicationForTable(publicationName, tableList, connection);
    }

    /**
     * Alter table replica identity full
     *
     * @param schema schema name
     * @param table table name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void alterTableReplicaIdentityFull(String schema, String table, Connection connection)
            throws SQLException {
        PgsqlUtils.alterTableReplicaIdentityFull(schema, table, connection);
    }

    /**
     * Alter table replica identity default
     *
     * @param schema schema name
     * @param table table name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void alterTableReplicaIdentityDefault(String schema, String table, Connection connection)
            throws SQLException {
        PgsqlUtils.alterTableReplicaIdentityDefault(schema, table, connection);
    }
}
