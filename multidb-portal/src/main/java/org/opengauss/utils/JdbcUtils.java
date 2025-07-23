/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.opengauss.domain.model.DatabaseConnectInfo;
import org.opengauss.domain.model.OpenGaussDatabaseConnectInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Jdbc utils
 *
 * @since 2025/5/16
 */
public class JdbcUtils {
    /**
     * Get mysql connection
     *
     * @param databaseConnectInfo database connect info
     * @return Connection
     * @throws ClassNotFoundException class not found exception
     * @throws SQLException sql exception
     */
    public static Connection getMysqlConnection(DatabaseConnectInfo databaseConnectInfo)
            throws ClassNotFoundException, SQLException {
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&characterEncoding=utf8",
                databaseConnectInfo.getIp(), databaseConnectInfo.getPort(), databaseConnectInfo.getDatabaseName());

        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(url, databaseConnectInfo.getUsername(), databaseConnectInfo.getPassword());
    }

    /**
     * Get mysql connection
     *
     * @param mysqlIp mysql ip
     * @param mysqlPort mysql port
     * @param databaseName database name
     * @param username username
     * @param password password
     * @return Connection
     * @throws ClassNotFoundException class not found exception
     * @throws SQLException sql exception
     */
    public static Connection getMysqlConnection(
            String mysqlIp, String mysqlPort, String databaseName, String username, String password)
            throws ClassNotFoundException, SQLException {
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&characterEncoding=utf8",
                mysqlIp, mysqlPort, databaseName);
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * Get openGauss connection
     *
     * @param opengaussIp openGauss info
     * @param opengaussPort openGauss port
     * @param databaseName database name
     * @param username username
     * @param password password
     * @return Connection
     * @throws SQLException sql exception
     */
    public static Connection getOpengaussConnection(
            String opengaussIp, String opengaussPort, String databaseName, String username, String password)
            throws SQLException {
        String url = String.format("jdbc:opengauss://%s:%s/%s", opengaussIp, opengaussPort, databaseName);
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * Get openGauss connection
     *
     * @param databaseConnectInfo database connect info
     * @return Connection
     * @throws SQLException sql exception
     */
    public static Connection getOpengaussConnection(OpenGaussDatabaseConnectInfo databaseConnectInfo)
            throws SQLException {
        StringBuilder urlBuilder = new StringBuilder("jdbc:opengauss://");
        urlBuilder.append(databaseConnectInfo.getIp()).append(":").append(databaseConnectInfo.getPort());
        if (databaseConnectInfo.isClusterAvailable()) {
            String[] standbyHosts = databaseConnectInfo.getStandbyHosts().split(",");
            String[] standbyPorts = databaseConnectInfo.getStandbyPorts().split(",");
            for (int i = 0; i < standbyHosts.length; i++) {
                urlBuilder.append(",").append(standbyHosts[i]).append(":").append(standbyPorts[i]);
            }
            urlBuilder.append("/").append(databaseConnectInfo.getDatabaseName()).append("?targetServerType=master");
        } else {
            urlBuilder.append("/").append(databaseConnectInfo.getDatabaseName());
        }
        String url = urlBuilder.toString();

        return DriverManager.getConnection(url, databaseConnectInfo.getUsername(), databaseConnectInfo.getPassword());
    }

    /**
     * Get pgsql connection
     *
     * @param databaseConnectInfo database connect info
     * @return Connection
     * @throws ClassNotFoundException class not found exception
     * @throws SQLException sql exception
     */
    public static Connection getPgsqlConnection(DatabaseConnectInfo databaseConnectInfo)
            throws ClassNotFoundException, SQLException {
        String url = String.format("jdbc:postgresql://%s:%s/%s", databaseConnectInfo.getIp(),
                databaseConnectInfo.getPort(), databaseConnectInfo.getDatabaseName());

        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, databaseConnectInfo.getUsername(),
                databaseConnectInfo.getPassword());
    }

    /**
     * Get pgsql connection
     *
     * @param pgsqlIp pgsql ip
     * @param pgsqlPort pgsql port
     * @param databaseName database name
     * @param username username
     * @param password password
     * @return Connection
     * @throws ClassNotFoundException class not found exception
     * @throws SQLException sql exception
     */
    public static Connection getPgsqlConnection(
            String pgsqlIp, String pgsqlPort, String databaseName, String username, String password)
            throws ClassNotFoundException, SQLException {
        String url = String.format("jdbc:postgresql://%s:%s/%s", pgsqlIp, pgsqlPort, databaseName);
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, username, password);
    }
}
