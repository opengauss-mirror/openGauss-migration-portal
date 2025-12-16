/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.SqlConstants;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mysql auth plugin verify chain
 *
 * @since 2025/6/7
 */
public class MysqlAuthPluginVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlAuthPluginVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Connect User Authentication Plugin Verify";

    @Override
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        MysqlMigrationConfigDto migrationConfigDto = verifyDto.getMigrationConfigDto();
        Connection connection = verifyDto.getMysqlConnection();
        try (PreparedStatement statement = connection.prepareStatement(SqlConstants.MYSQL_SELECT_USER_AUTH_PLUGIN)) {
            statement.setString(1, migrationConfigDto.getMysqlDatabaseUsername());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String plugin = resultSet.getString("plugin");
                    String expectedPlugin = "mysql_native_password";
                    if (expectedPlugin.equals(plugin)) {
                        chainResult.setSuccess(true);
                    } else {
                        chainResult.setSuccess(false);
                        chainResult.setDetail(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL,
                                "user authentication plugin", migrationConfigDto.getOpengaussDatabaseIp(), plugin));
                    }
                }
            }
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }

        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
