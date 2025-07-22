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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * pgsql utils
 *
 * @since 2025/6/24
 */
public class PgsqlUtils {
    private static final Pattern PG_VERSION_PATTERN = Pattern.compile("PostgreSQL\\s((?:\\d+\\.)+[^\\s]*) ");

    /**
     * Get pgsql version
     *
     * @param connection pgsql connection
     * @return pgsql version
     * @throws SQLException sql exception
     */
    public static String getPgsqlVersion(Connection connection) throws SQLException {
        String sql = "SELECT version();";
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                String rsString = rs.getString("version");
                Matcher matcher = PG_VERSION_PATTERN.matcher(rsString);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
        }

        throw new SQLException("Not found PostgreSQL version.");
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
        List<String> result = new ArrayList<>();

        String selectSql = String.format("SELECT distinct(tablename) FROM pg_tables WHERE SCHEMANAME = '%s';",
                schema);
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(selectSql)
        ) {
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                result.add(tableName);
            }
        }
        return result;
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
        String sql = String.format(SqlConstants.SHOW_VARIABLE, variableName);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        throw new SQLException("Variable " + variableName + " not found");
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
        boolean hasSchema = false;
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.IS_SCHEMA_EXISTS)) {
            statement.setString(1, schemaName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String isExists = resultSet.getString(1);
                    hasSchema = isExists.equals("1") || isExists.equals("t");
                }
            }
        }
        return hasSchema;
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
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.CREATE_REPLICATION_SLOT)) {
            statement.setString(1, slotName);
            statement.setString(2, plugin);
            statement.execute();
        }
    }

    /**
     * Drop a replication slot
     *
     * @param slotName slot name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void dropReplicationSlot(String slotName, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.DROP_REPLICATION_SLOT)) {
            statement.setString(1, slotName);
            statement.execute();
        }
    }

    /**
     * Get the replication slot names
     *
     * @param connection connection
     * @return replication slot names list
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getReplicationSlotNames(Connection connection) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SqlConstants.SELECT_REPLICATION_SLOT_NAMES)) {
            while (resultSet.next()) {
                String slotName = resultSet.getString("slot_name");
                result.add(slotName);
            }
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Get the number of replication slots
     *
     * @param connection connection
     * @return number of replication slots
     * @throws SQLException if a database access error occurs
     */
    public static int countReplicationSlots(Connection connection) throws SQLException {
        int result = 0;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SqlConstants.COUNT_REPLICATION_SLOTS)) {
            if (resultSet.next()) {
                result = resultSet.getInt(1);
            }
        }
        return result;
    }

    /**
     * Get the publication names
     *
     * @param connection connection
     * @return publication names list
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getPublicationNames(Connection connection) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SqlConstants.SELECT_PUBLICATION_NAMES)) {
            while (resultSet.next()) {
                String publicationName = resultSet.getString("pubname");
                result.add(publicationName);
            }
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Drop a publication
     *
     * @param publicationName publication name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void dropPublication(String publicationName, Connection connection) throws SQLException {
        String sql = String.format(SqlConstants.DROP_PUBLICATION, publicationName);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Create publication for all tables
     *
     * @param publicationName publication name
     * @param connection connection
     * @throws SQLException if a database access error occurs
     */
    public static void createPublicationAllTables(String publicationName, Connection connection) throws SQLException {
        String sql = String.format(SqlConstants.CREATE_PUBLICATION_ALL_TABLES, publicationName);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
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
        String tables = String.join(",", tableList);
        String sql = String.format(SqlConstants.CREATE_PUBLICATION_FOR_TABLE, publicationName, tables);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
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
        String sql = String.format(SqlConstants.ALTER_TABLE_REPLICA_IDENTITY_FULL, schema, table);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
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
        String sql = String.format(SqlConstants.ALTER_TABLE_REPLICA_IDENTITY_DEFAULT, schema, table);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
