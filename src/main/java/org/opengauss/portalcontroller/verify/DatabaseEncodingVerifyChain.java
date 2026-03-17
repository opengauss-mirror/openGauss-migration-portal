/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Verify database encoding
 *
 * @since 2026/3/17
 */
public class DatabaseEncodingVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseEncodingVerifyChain.class);

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        doVerify(resultMap, mysqlConnection, pgConnection);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void doVerify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        String verifyKey = "database_encoding";
        Map<String, Object> paramMap = new HashMap<>();
        if (pgConnection == null || mysqlConnection == null) {
            resultMap.put(verifyKey, paramMap);
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
            setVerifyResult(resultMap, false);
            return;
        }

        boolean isPassedVerify = false;
        try {
            if (!isOpenGaussLaterThan700RC3(pgConnection)) {
                return;
            }
            String mysqlEncoding = getMysqlDatabaseEncoding(mysqlConnection);
            if (!mysqlEncoding.toLowerCase(Locale.ENGLISH).contains("utf8")) {
                return;
            }

            resultMap.put(verifyKey, paramMap);
            String openGaussEncoding = getOpenGaussDatabaseEncoding(pgConnection);
            if (openGaussEncoding.equalsIgnoreCase("utf8")) {
                paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
                isPassedVerify = true;
            } else {
                paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                String msg = String.format("MySQL database encoding is %s, openGauss database encoding should be UTF8, "
                        + "current is %s. You can create openGauss database with encoding UTF8 by sql: "
                        + "\"CREATE DATABASE db_name WITH DBCOMPATIBILITY = 'B' ENCODING = 'UTF8';\"",
                        mysqlEncoding, openGaussEncoding);
                paramMap.put("message", msg);
            }
        } catch (SQLException e) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message", e.getMessage());
            LOGGER.error("Failed to verify database encoding", e);
        }
        setVerifyResult(resultMap, isPassedVerify);
    }

    private String getMysqlDatabaseEncoding(Connection mysqlConnection) throws SQLException {
        String sql = "SELECT @@character_set_database;";
        try (var stmt = mysqlConnection.createStatement();
             var rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to get MySQL database encoding", e);
            throw new SQLException("Failed to get MySQL database encoding, error: " + e.getMessage());
        }
        throw new SQLException("Not found MySQL database encoding by sql: " + sql);
    }

    private String getOpenGaussDatabaseEncoding(PgConnection pgConnection) throws SQLException {
        String sql = "SHOW server_encoding;";
        try (var stmt = pgConnection.createStatement();
             var rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to get openGauss database encoding", e);
            throw new SQLException("Failed to get openGauss database encoding, error: " + e.getMessage());
        }
        throw new SQLException("Not found openGauss database encoding by sql: " + sql);
    }
}
