/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.OpenGaussUtils;

import java.sql.SQLException;

/**
 * opengauss wal_level Verify Chain
 *
 * @since 2025/6/7
 */
public class OpenGaussWalLevelVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussWalLevelVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss GUC Parameter wal_level Verify";

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        try {
            String param = "wal_level";
            String expectValue = "logical";
            String actualValue = OpenGaussUtils.getVariableValue(param, verifyDto.getTargetConnection());
            if (!expectValue.equals(actualValue)) {
                chainResult.setSuccess(false);
                chainResult.setDetail(String.format(VerifyConstants.VERIFY_FAILED_RESULT_MODEL, param, expectValue,
                        actualValue));
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
