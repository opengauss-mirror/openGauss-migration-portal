/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.database.connect.info.DatabaseConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mysql connect verify chain
 *
 * @since 2025/7/9
 */
public class MysqlConnectVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlConnectVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Connect Verify";

    @Override
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        DatabaseConnectInfo mysqlConnectInfo = verifyDto.getMigrationConfigDto().getMysqlConnectInfo();
        try {
            Connection connection = JdbcUtils.getMysqlConnection(mysqlConnectInfo);
            verifyDto.setMysqlConnection(connection);
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        } catch (ClassNotFoundException e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        } finally {
            addCurrentChainResult(verifyResult);
        }

        transfer(verifyDto, verifyResult);
    }
}
