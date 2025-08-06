/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.utils;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.opengauss.portalcontroller.verify.FullPermissionVerifyChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JdbcUtils
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class JdbcUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtils.class);

    /**
     * Gets mysql connection.
     *
     * @return the mysql connection
     * @throws PortalException the portal exception
     */
    public static Connection getMysqlConnection() {
        String ip = PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_HOST);
        String port = PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_PORT);
        String databaseName = PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + databaseName + "?useSSL=false";
        String user = PortalControl.toolsMigrationParametersTable.get(Mysql.USER);
        String password = PortalControl.toolsMigrationParametersTable.get(Mysql.PASSWORD);
        String driver = "com.mysql.cj.jdbc.Driver";
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            LOGGER.error("{}Failed to get MySQL connection. Please check the connect info and MySQL status",
                    ErrorCode.SQL_EXCEPTION, e);
        }
        return connection;
    }

    /**
     * Gets current uuid.
     *
     * @param connection the connection
     * @return the current uuid
     * @throws SQLException the sql exception
     */
    public static String getCurrentUuid(Connection connection) throws SQLException {
        String uuid;
        String selectReadOnlySql = "show variables like 'read_only';";
        String readOnlyColumnName = "Value";
        String checkMaster = selectStringValue(connection, selectReadOnlySql, readOnlyColumnName);
        if (checkMaster.equals("OFF")) {
            String masterSelectSql = "show global variables like 'server_uuid';";
            String masterColumnName = "Value";
            uuid = selectStringValue(connection, masterSelectSql, masterColumnName);
        } else {
            String slaveSelectSql = "show slave status;";
            String slaveColumnName = "Master_UUID";
            uuid = selectStringValue(connection, slaveSelectSql, slaveColumnName);
        }
        return uuid;
    }

    /**
     * Select string value string.
     *
     * @param connection the connection
     * @param selectSql  the select sql
     * @param key        the key
     * @return the string
     * @throws SQLException the sql exception
     */
    public static String selectStringValue(Connection connection, String selectSql, String key) throws SQLException {
        String value = "";
        if (connection != null) {
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(selectSql)) {
                if (rs.next()) {
                    value = rs.getString(key);
                }
            } catch (SQLException e) {
                throw e;
            }
        }
        return value;
    }

    /**
     * Select string value string.
     *
     * @param connection the connection
     * @param selectSql  the select sql
     * @param columnKeys the key
     * @return Map<String, String>
     */
    public static Map<String, String> selectMapValue(Connection connection, String selectSql, String[] columnKeys) {
        Map<String, String> resultMap = new HashMap<>();
        if (connection != null) {
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(selectSql)) {
                int columnCount = rs.getMetaData().getColumnCount();
                if (rs.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        resultMap.put(columnKeys[i], rs.getString(columnKeys[i]));
                    }
                }
            } catch (SQLException e) {
                LOGGER.error("Execute {} failed", selectSql, e);
            }
        }
        return resultMap;
    }

    /**
     * Gets pg connection.
     *
     * @return the pg connection
     */
    public static PgConnection getPgConnection() {
        PgConnection conn = null;
        Hashtable<String, String> hashtable = PortalControl.toolsMigrationParametersTable;
        try {
            conn = (PgConnection) DriverManager.getConnection(
                    getOpengaussJdbcUrl(), hashtable.get(Opengauss.USER), hashtable.get(Opengauss.PASSWORD));
        } catch (SQLException e) {
            LOGGER.error("{}Failed to get openGauss connection. Please check the connect info and openGauss status",
                    ErrorCode.SQL_EXCEPTION, e);
        }
        return conn;
    }

    /**
     * get openGauss jdbc url
     *
     * @return String
     */
    public static String getOpengaussJdbcUrl() {
        Hashtable<String, String> hashtable = PortalControl.toolsMigrationParametersTable;
        String opengaussDatabaseHost = hashtable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = hashtable.get(Opengauss.DATABASE_PORT);
        String opengaussDatabaseName = hashtable.get(Opengauss.DATABASE_NAME);

        StringBuilder urlBuilder = new StringBuilder("jdbc:opengauss://");
        urlBuilder.append(opengaussDatabaseHost).append(":").append(opengaussDatabasePort);
        if (Opengauss.isOpengaussClusterAvailable()) {
            Map<String, String[]> standbyInformationMap = Opengauss.getStandbyInformationMap();
            String[] standbyHosts = standbyInformationMap.get(Opengauss.DATABASE_STANDBY_HOSTS);
            String[] standbyPorts = standbyInformationMap.get(Opengauss.DATABASE_STANDBY_PORTS);
            for (int i = 0; i < standbyHosts.length; i++) {
                urlBuilder.append(",").append(standbyHosts[i]).append(":").append(standbyPorts[i]);
            }
            urlBuilder.append("/").append(opengaussDatabaseName).append("?targetServerType=master");
        } else {
            urlBuilder.append("/").append(opengaussDatabaseName);
        }
        return urlBuilder.toString();
    }

    /**
     * Select global variables boolean.
     *
     * @param connection   the connection
     * @param columnName   the key
     * @param defaultValue the default value
     * @return the boolean
     */
    public static boolean selectGlobalVariables(PgConnection connection, String columnName, String defaultValue) {
        boolean flag = false;
        String sql = "show " + columnName + ";";
        try {
            String value = selectStringValue(connection, sql, columnName);
            if (value.equals(defaultValue)) {
                flag = true;
            } else {
                String reason = "If you want to use reverse migration,"
                        + "please alter system set " + columnName + " to " + defaultValue + " "
                        + "and restart openGauss to make it work.";
                ReverseMigrationTool.refuseReverseMigrationReason = reason;
            }
        } catch (SQLException e) {
            PortalException portalException = new PortalException("SQL exception", "select global variable",
                    e.getMessage());
            portalException.setRequestInformation("Select global variable " + columnName + " failed.");
            ReverseMigrationTool.refuseReverseMigrationReason = portalException.getMessage();
            LOGGER.error("{}Failed to select openGauss global variable '{}'", ErrorCode.SQL_EXCEPTION, columnName, e);
        }
        return flag;
    }

    /**
     * Select version boolean.
     *
     * @param connection the connection
     * @return the boolean
     */
    public static boolean selectVersion(PgConnection connection) {
        boolean flag = false;
        if (connection != null) {
            String selectVersionSql = "select version()";
            String versionColumnName = "version";
            try {
                String value = selectStringValue(connection, selectVersionSql, versionColumnName);
                if (value.contains("openGauss") && value.contains("build")) {
                    String openGauss = "openGauss";
                    int startIndex = value.indexOf(openGauss) + openGauss.length();
                    int endIndex = value.indexOf("build");
                    String version = value.substring(startIndex, endIndex).trim();
                    int versionNum = Integer.parseInt(version.replaceAll("\\.", ""));
                    if (versionNum >= 300) {
                        flag = true;
                    } else {
                        String reason = "Please upgrade openGauss to 3.0.0 or higher to use reverse migration.";
                        ReverseMigrationTool.refuseReverseMigrationReason = reason;
                        LOGGER.error(reason);
                    }
                } else {
                    flag = true;
                }
            } catch (SQLException e) {
                PortalException portalException = new PortalException("SQL exception", "select openGauss version",
                        e.getMessage());
                portalException.setRequestInformation("Select openGauss version failed.");
                ReverseMigrationTool.refuseReverseMigrationReason = portalException.getMessage();
                LOGGER.error(portalException.toString());
            }
        }
        return flag;
    }

    /**
     * Get the table list in migration schema
     *
     * @param connection connection
     * @return table list
     */
    public static List<String> getMigrationSchemaTables(PgConnection connection) {
        List<String> result = new ArrayList<>();
        if (connection != null) {
            String schema = PortalControl.toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA);
            try {
                connection.setSchema(schema);
            } catch (SQLException e) {
                LOGGER.warn("Failed to set connect schema{}", e.getMessage());
            }

            String selectSql = "SELECT distinct(tablename) FROM pg_tables WHERE SCHEMANAME = '" + schema + "';";
            try (Statement selectTableStatement = connection.createStatement();
                 ResultSet rs = selectTableStatement.executeQuery(selectSql)
            ) {
                while (rs.next()) {
                    String tableName = rs.getString("tablename");
                    result.add(tableName);
                }
            } catch (SQLException e) {
                LOGGER.error("{}Failed to select schema tables, sql: {}", ErrorCode.SQL_EXCEPTION, selectSql, e);
            }
        }
        return result;
    }

    /**
     * Alter table replica identity full
     *
     * @param connection connection
     * @param schemaTables table list
     */
    public static void changeAllTable(PgConnection connection, List<String> schemaTables) {
        if (connection != null) {
            try (Statement alterTableStatement = connection.createStatement()) {
                for (String tableName : schemaTables) {
                    String alterTableSql = String.format("ALTER table \"%s\" replica identity full;", tableName);
                    alterTableStatement.execute(alterTableSql);
                }
                LOGGER.info("Alter all table replica identity full finished.");
            } catch (SQLException e) {
                LOGGER.error("Failed to alter openGauss table replica identity to full", e);
            }
        }
    }

    /**
     * Create logical replication slot boolean.
     *
     * @param connection the connection
     */
    public static void createLogicalReplicationSlot(PgConnection connection) {
        String slotName = "slot_" + Plan.workspaceId;
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                String selectSlotSql = "SELECT * FROM pg_get_replication_slots()";
                String columnName = "slot_name";
                boolean isReplicationSlotExists = isSpecifiedNameExist(statement, selectSlotSql, slotName, columnName);
                // If the slot does not exist, create a new slot.
                if (!isReplicationSlotExists) {
                    Set<String> pluginNameMap = PortalControl.toolsMigrationParametersTable.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().startsWith("8") && entry.getKey()
                            .substring(4)
                            .equals("plugin.name"))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toSet());
                    String pluginName = pluginNameMap.iterator().next();
                    String createSlotSql = "SELECT * FROM pg_create_logical_replication_slot('" + slotName + "', " + "'"
                        + pluginName + "')";
                    statement.execute(createSlotSql);
                }
                Plan.slotName = slotName;
                LOGGER.info("Create logical replication slot " + slotName + " finished.");
            } catch (SQLException e) {
                LOGGER.error("Failed to create logical replication slot '{}'", slotName, e);
            }
        }
    }

    /**
     * Create publication
     *
     * @param connection connection
     * @param schemaTables schema table list
     */
    public static void createPublication(PgConnection connection, List<String> schemaTables) {
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                String selectPublicationSql = "SELECT pubname from pg_publication";
                String publicationName = "dbz_publication";
                String pubName = "pubname";
                boolean isPublicationExist = isSpecifiedNameExist(statement, selectPublicationSql, publicationName,
                        pubName);

                if (isPublicationExist) {
                    LOGGER.info("PUBLICATION dbz_publication already exists.");
                } else {
                    String createSql = "CREATE PUBLICATION dbz_publication FOR ALL TABLES;";
                    boolean systemAdmin = FullPermissionVerifyChain.judgeSystemAdmin(connection);
                    if (!systemAdmin) {
                        String tables = String.join(",", schemaTables);
                        createSql = String.format("CREATE PUBLICATION dbz_publication FOR TABLE %s;", tables);
                    }

                    statement.execute(createSql);
                    LOGGER.info("Create publication dbz_publication finished.");
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to create publication", e);
            }
        }
    }

    /**
     * Is specified name exist boolean.
     *
     * @param statement  the statement
     * @param sql        the sql
     * @param name       the name
     * @param columnName the column name
     * @return the boolean
     */
    public static boolean isSpecifiedNameExist(Statement statement, String sql, String name, String columnName) {
        boolean flag = false;
        try {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    if (resultSet.getString(columnName).equals(name)) {
                        flag = true;
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("{}Failed to execute sql: {}", ErrorCode.SQL_EXCEPTION, sql, e);
        }
        return flag;
    }

    /**
     * Drop logical replication slot boolean.
     *
     * @param connection the connection
     */
    public static void dropLogicalReplicationSlot(PgConnection connection) {
        if (connection != null) {
            String slotName = Plan.slotName;
            try (Statement statement = connection.createStatement()) {
                String selectSlotSql = "SELECT * FROM pg_get_replication_slots()";
                String columnName = "slot_name";
                boolean isReplicationSlotExists = isSpecifiedNameExist(statement, selectSlotSql, slotName,
                        columnName);
                if (isReplicationSlotExists) {
                    String createSlotSql = "SELECT * FROM pg_drop_replication_slot('" + slotName + "')";
                    statement.execute(createSlotSql);
                    LOGGER.info("Drop logical replication slot '{}' finished.", slotName);
                } else {
                    LOGGER.info("No logical replication slot '{}' to drop.", slotName);
                }
                String selectPublicationSql = "SELECT pubname from pg_publication";
                String publicationName = "dbz_publication";
                String pubName = "pubname";
                boolean isPublicationExist = isSpecifiedNameExist(statement, selectPublicationSql, publicationName,
                        pubName);
                if (isPublicationExist) {
                    String createPublicationSql = "DROP PUBLICATION " + publicationName;
                    statement.execute(createPublicationSql);
                    LOGGER.info("Drop publication " + publicationName + " finished.");
                } else {
                    LOGGER.info("PUBLICATION " + publicationName + " does not exist.");
                }
            } catch (SQLException e) {
                LOGGER.error("{}Failed to drop logical replication slot '{}'", ErrorCode.SQL_EXCEPTION, slotName, e);
            }
        }
    }

    /**
     * close connection
     *
     * @param connection connection
     */
    public static void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("close connection fail.");
        }
    }

    /**
     * query param
     *
     * @param pgConnection         target connection
     * @param databaseKernelParams paramMap
     * @return resultMap
     */
    public static Map<String, Object> queryParam(Connection pgConnection,
                                                 Hashtable<String, String> databaseKernelParams) {
        String paramValue;
        Map<String, Object> resultMap = new HashMap<>();
        try {
            for (String key : databaseKernelParams.keySet()) {
                String selectSql = "show variables like '" + key + "'";
                paramValue = JdbcUtils.selectStringValue(pgConnection, selectSql, "Value");
                LOGGER.info("param {} is {}", key, paramValue);
                resultMap.put(key, paramValue);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query openGauss param", e);
        }
        return resultMap;
    }

    /**
     * query param
     *
     * @param pgConnection         target connection
     * @param databaseKernelParams paramMap
     */
    public static void adjustDatabaseParam(Connection pgConnection, Hashtable<String, String> databaseKernelParams) {
        for (String key : databaseKernelParams.keySet()) {
            String selectSql = "alter system set " + key + " to " + databaseKernelParams.get(key);
            JdbcUtils.executeSql(pgConnection, selectSql);
        }
    }

    /**
     * execute sql ,not result set.
     *
     * @param connection the connection
     * @param sql        sql
     */
    public static void executeSql(Connection connection, String sql) {
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            } catch (SQLException e) {
                LOGGER.error("{}Failed to execute sql: {}", ErrorCode.SQL_EXCEPTION, sql, e);
            }
        }
    }
}
