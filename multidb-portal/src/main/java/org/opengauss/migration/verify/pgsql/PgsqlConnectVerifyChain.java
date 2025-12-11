/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.pgsql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.PgsqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * PostgreSQL connect verify chain
 *
 * @since 2025/7/9
 */
public class PgsqlConnectVerifyChain extends AbstractPgsqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlConnectVerifyChain.class);
    private static final String VERIFY_NAME = "PostgreSQL Connect Verify";

    @Override
    public void doVerify(PgsqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        PgsqlMigrationConfigDto migrationConfigDto = verifyDto.getMigrationConfigDto();
        try {
            Connection connection = JdbcUtils.getPgsqlConnection(
                    migrationConfigDto.getPgsqlDatabaseIp(), migrationConfigDto.getPgsqlDatabasePort(),
                    migrationConfigDto.getPgsqlDatabaseName(), migrationConfigDto.getPgsqlDatabaseUsername(),
                    migrationConfigDto.getPgsqlDatabasePassword());
            verifyDto.setPgsqlConnection(connection);
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
            return;
        } catch (ClassNotFoundException e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
            return;
        } finally {
            addCurrentChainResult(verifyResult);
        }

        transfer(verifyDto, verifyResult);
    }
}
