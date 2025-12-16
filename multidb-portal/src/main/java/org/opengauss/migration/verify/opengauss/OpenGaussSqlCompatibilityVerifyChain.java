/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.OpenGaussUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * openGauss SQL Compatibility Verify Chain
 *
 * @since 2025/6/7
 */
public class OpenGaussSqlCompatibilityVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussSqlCompatibilityVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Database sql_compatibility Verify";
    private static final String SQL_COMPATIBILITY_PARAM = "sql_compatibility";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            Connection targetConnection = verifyDto.getOpengaussConnection();
            String sqlCompatibility = OpenGaussUtils.getVariableValue(SQL_COMPATIBILITY_PARAM, targetConnection);

            String expectValue;
            DatabaseType sourceDbType = verifyDto.getSourceDbType();
            if (DatabaseType.MYSQL.equals(sourceDbType)) {
                expectValue = "B";
                compareSqlCompatibility(expectValue, sqlCompatibility);
            } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
                expectValue = "PG";
                compareSqlCompatibility(expectValue, sqlCompatibility);
            } else if (DatabaseType.ELASTICSEARCH.equals(sourceDbType) || DatabaseType.MILVUS.equals(sourceDbType)) {
                expectValue = "A";
                compareSqlCompatibility(expectValue, sqlCompatibility);
            } else {
                chainResult.setSuccess(false);
                chainResult.setDetail("Unsupported source database type: " + sourceDbType);
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

    private void compareSqlCompatibility(String expectValue, String sqlCompatibility) {
        if (!expectValue.equalsIgnoreCase(sqlCompatibility)) {
            chainResult.setSuccess(false);
            chainResult.setDetail(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, SQL_COMPATIBILITY_PARAM,
                    expectValue, sqlCompatibility));
        }
    }
}
