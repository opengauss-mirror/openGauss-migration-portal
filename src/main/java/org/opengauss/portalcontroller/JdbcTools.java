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

package org.opengauss.portalcontroller;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.exception.PortalException;
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
import java.util.Map;

/**
 * The type Jdbc tools.
 */
public class JdbcTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTools.class);

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
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + databaseName;
        String user = PortalControl.toolsMigrationParametersTable.get(Mysql.USER);
        String password = PortalControl.toolsMigrationParametersTable.get(Mysql.PASSWORD);
        String driver = "com.mysql.cj.jdbc.Driver";
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
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
     * @param columnKeys        the key
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
                LOGGER.error("execute {} failed", selectSql);
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
        String opengaussDatabaseHost = hashtable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = hashtable.get(Opengauss.DATABASE_PORT);
        String opengaussDatabaseName = hashtable.get(Opengauss.DATABASE_NAME);
        String opengaussUserName = hashtable.get(Opengauss.USER);
        String opengaussUserPassword = hashtable.get(Opengauss.PASSWORD);
        String opengaussUrl = "jdbc:opengauss://" + opengaussDatabaseHost + ":" +
                opengaussDatabasePort + "/" + opengaussDatabaseName;
        try {
            conn = (PgConnection) DriverManager.getConnection(opengaussUrl, opengaussUserName, opengaussUserPassword);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
        return conn;
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
                String reason = "If you want to use reverse migration," +
                        "please alter system set " + columnName + " to " + defaultValue + " " +
                        "and restart openGauss to make it work.";
                PortalControl.refuseReverseMigrationReason = reason;
                LOGGER.error(reason);
            }
        } catch (SQLException e) {
            PortalException portalException = new PortalException("SQL exception", "select global variable", e.getMessage());
            portalException.setRequestInformation("Select global variable " + columnName + " failed.");
            PortalControl.refuseReverseMigrationReason = portalException.getMessage();
            LOGGER.error(portalException.toString());
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
                        PortalControl.refuseReverseMigrationReason = reason;
                        LOGGER.error(reason);
                    }
                } else {
                    flag = true;
                }
            } catch (SQLException e) {
                PortalException portalException = new PortalException("SQL exception", "select openGauss version", e.getMessage());
                portalException.setRequestInformation("Select openGauss version failed.");
                PortalControl.refuseReverseMigrationReason = portalException.getMessage();
                LOGGER.error(portalException.toString());
            }
        }
        return flag;
    }

    /**
     * Change all table boolean.
     *
     * @param connection the connection
     */
    public static void changeAllTable(PgConnection connection) {
        if (connection != null) {
            String schema = PortalControl.toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA);
            try {
                connection.setSchema(schema);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
            String selectSql = "SELECT distinct(tablename) FROM pg_tables WHERE SCHEMANAME = '" + schema + "';";
            try (Statement selectTableStatement = connection.createStatement(); ResultSet rs = selectTableStatement.executeQuery(selectSql); Statement alterTableStatement = connection.createStatement()) {
                ArrayList<String> arrayList = new ArrayList<>();
                while (rs.next()) {
                    String tableName = rs.getString("tablename");
                    arrayList.add(tableName);
                }
                for (String tableName : arrayList) {
                    String alterTableSql = String.format("ALTER table %s replica identity full", tableName);
                    alterTableStatement.execute(alterTableSql);
                }
                LOGGER.info("Alter all table replica identity full finished.");
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
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
                if (isReplicationSlotExists) {
                    slotName += "_" + System.currentTimeMillis();
                }
                String createSlotSql = "SELECT * FROM pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')";
                statement.execute(createSlotSql);
                Plan.slotName = slotName;
                LOGGER.info("Create logical replication slot " + slotName + " finished.");
                String selectPublicationSql = "SELECT pubname from pg_publication";
                String publicationName = "dbz_publication";
                String pubName = "pubname";
                boolean isPublicationExist = isSpecifiedNameExist(statement, selectPublicationSql, publicationName, pubName);
                if (isPublicationExist) {
                    LOGGER.info("PUBLICATION dbz_publication already exists.");
                } else {
                    String createPublicationSql = "CREATE PUBLICATION dbz_publication FOR ALL TABLES;";
                    statement.execute(createPublicationSql);
                    LOGGER.info("Create publication dbz_publication finished.");
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
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
            LOGGER.error(e.getMessage());
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
            try (Statement statement = connection.createStatement()) {
                String selectSlotSql = "SELECT * FROM pg_get_replication_slots()";
                String columnName = "slot_name";
                boolean isReplicationSlotExists = isSpecifiedNameExist(statement, selectSlotSql, Plan.slotName, columnName);
                if (isReplicationSlotExists) {
                    String createSlotSql = "SELECT * FROM pg_drop_replication_slot('" + Plan.slotName + "')";
                    statement.execute(createSlotSql);
                    LOGGER.info("Drop logical replication slot " + Plan.slotName + " finished.");
                } else {
                    LOGGER.info("No logical replication slot " + Plan.slotName + " to drop.");
                }
                String selectPublicationSql = "SELECT pubname from pg_publication";
                String publicationName = "dbz_publication";
                String pubName = "pubname";
                boolean isPublicationExist = isSpecifiedNameExist(statement, selectPublicationSql, publicationName, pubName);
                if (isPublicationExist) {
                    String createPublicationSql = "DROP PUBLICATION " + publicationName;
                    statement.execute(createPublicationSql);
                    LOGGER.info("Drop publication " + publicationName + " finished.");
                } else {
                    LOGGER.info("PUBLICATION " + publicationName + " does not exist.");
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
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
}
