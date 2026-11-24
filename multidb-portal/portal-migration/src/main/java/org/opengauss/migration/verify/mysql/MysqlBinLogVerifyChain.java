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
import org.opengauss.migration.domain.config.MysqlMigrationConfig;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.migration.utils.MysqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mysql bin log verify chain
 *
 * @since 2025/6/7
 */
public class MysqlBinLogVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlBinLogVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Bin Log Variables Verify";

    @Override
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        MysqlMigrationConfig migrationConfigDto = verifyDto.getMigrationConfigDto();
        Connection connection = verifyDto.getMysqlConnection();
        try {
            StringBuilder detailBuilder = new StringBuilder();

            String logBinVariable = "log_bin";
            String logBinExpectedValue = "ON";
            String logBinValue = MysqlUtils.getVariableValue(logBinVariable, connection);
            if (!logBinValue.equals(logBinExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, logBinVariable,
                        migrationConfigDto.getOpengaussDatabaseIp(), logBinValue)).append("; ");
            }

            String binlogFormatVariable = "binlog_format";
            String binlogFormatExpectedValue = "ROW";
            String binlogFormatValue = MysqlUtils.getVariableValue(binlogFormatVariable, connection);
            if (!binlogFormatValue.equals(binlogFormatExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, binlogFormatVariable,
                        migrationConfigDto.getOpengaussDatabaseIp(), binlogFormatValue)).append("; ");
            }

            String binlogRowImageVariable = "binlog_row_image";
            String binlogRowImageExpectedValue = "FULL";
            String binlogRowImageValue = MysqlUtils.getVariableValue(binlogRowImageVariable, connection);
            if (!binlogRowImageValue.equals(binlogRowImageExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, binlogRowImageVariable,
                        migrationConfigDto.getOpengaussDatabaseIp(), binlogRowImageValue)).append("; ");
            }

            if (!chainResult.isSuccess()) {
                chainResult.setDetail(detailBuilder.substring(0, detailBuilder.length() - 2));
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
