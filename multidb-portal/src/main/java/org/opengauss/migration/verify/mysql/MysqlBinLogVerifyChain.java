/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MysqlUtils;

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
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        Connection connection = verifyDto.getSourceConnection();
        try {
            StringBuilder detailBuilder = new StringBuilder();

            String logBinVariable = "log_bin";
            String logBinExpectedValue = "ON";
            String logBinValue = MysqlUtils.getVariableValue(logBinVariable, connection);
            if (!logBinValue.equals(logBinExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, logBinVariable,
                        verifyDto.getTargetIp(), logBinValue)).append("; ");
            }

            String binlogFormatVariable = "binlog_format";
            String binlogFormatExpectedValue = "ROW";
            String binlogFormatValue = MysqlUtils.getVariableValue(binlogFormatVariable, connection);
            if (!binlogFormatValue.equals(binlogFormatExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, binlogFormatVariable,
                        verifyDto.getTargetIp(), binlogFormatValue)).append("; ");
            }

            String binlogRowImageVariable = "binlog_row_image";
            String binlogRowImageExpectedValue = "FULL";
            String binlogRowImageValue = MysqlUtils.getVariableValue(binlogRowImageVariable, connection);
            if (!binlogRowImageValue.equals(binlogRowImageExpectedValue)) {
                chainResult.setSuccess(false);
                detailBuilder.append(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, binlogRowImageVariable,
                        verifyDto.getTargetIp(), binlogRowImageValue)).append("; ");
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
