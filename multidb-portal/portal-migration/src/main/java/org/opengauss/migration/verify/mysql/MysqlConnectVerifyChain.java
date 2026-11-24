/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.domain.model.DatabaseConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.migration.utils.JdbcUtils;

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
