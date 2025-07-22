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
 * openGauss enable_slot_log verify chain
 *
 * @since 2025/6/7
 */
public class OpenGaussEnableSlotLogVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussEnableSlotLogVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss GUC Parameter enable_slot_log Verify";

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        if (verifyDto.isTargetCluster()) {
            doVerify(verifyDto);
            addCurrentChainResult(verifyResult);
        }
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(VerifyDto verifyDto) {
        try {
            String param = "enable_slot_log";
            String expectValue = "on";
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
    }
}
