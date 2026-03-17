/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MysqlUtils;
import org.opengauss.utils.OpenGaussUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * MySQL database encoding verify chain
 *
 * @since 2026/3/17
 */
public class MysqlDatabaseEncodingVerifyChain extends AbstractMysqlVerifyChain {
private static final Logger LOGGER = LogManager.getLogger(MysqlDatabaseEncodingVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Database Encoding Verify";

    @Override
    void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);
        verifyEncoding(verifyDto, verifyResult);
        transfer(verifyDto, verifyResult);
    }

    private void verifyEncoding(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        try {
            Connection opengaussConnection = verifyDto.getOpengaussConnection();
            if (!isOpenGaussLaterThan700RC3(opengaussConnection)) {
                return;
            }
            Connection mysqlConnection = verifyDto.getMysqlConnection();
            String mysqlEncoding = getMysqlDatabaseEncoding(mysqlConnection);
            if (!mysqlEncoding.toLowerCase(Locale.ENGLISH).contains("utf8")) {
                return;
            }

            String openGaussEncoding = getOpenGaussDatabaseEncoding(opengaussConnection);
            if (!openGaussEncoding.equalsIgnoreCase("utf8")) {
                chainResult.setSuccess(false);
                String msg = String.format("MySQL database encoding is %s, openGauss database encoding should be UTF8, "
                                + "current is %s. You can create openGauss database with encoding UTF8 by sql: "
                                + "\"CREATE DATABASE db_name WITH DBCOMPATIBILITY = 'B' ENCODING = 'UTF8';\"",
                        mysqlEncoding, openGaussEncoding);
                chainResult.setDetail(msg);
            }
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }
        addCurrentChainResult(verifyResult);
    }

    private String getMysqlDatabaseEncoding(Connection mysqlConnection) throws SQLException {
        try {
            return MysqlUtils.showDatabaseCharacterSet(mysqlConnection);
        } catch (SQLException e) {
            LOGGER.error("Failed to get MySQL database encoding", e);
            throw new SQLException("Failed to get MySQL database encoding, error: " + e.getMessage());
        }
    }

    private String getOpenGaussDatabaseEncoding(Connection connection) throws SQLException {
        try {
            return OpenGaussUtils.getVariableValue("server_encoding", connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to get openGauss database encoding", e);
            throw new SQLException("Failed to get openGauss database encoding, error: " + e.getMessage());
        }
    }
}
