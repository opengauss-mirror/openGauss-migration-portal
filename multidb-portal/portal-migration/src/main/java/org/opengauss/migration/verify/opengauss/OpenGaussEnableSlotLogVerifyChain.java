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

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.migration.utils.OpenGaussUtils;

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
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        if (verifyDto.getMigrationConfigDto().isOpenGaussClusterAvailable()) {
            doVerify(verifyDto);
            addCurrentChainResult(verifyResult);
        }
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(AbstractVerifyDto verifyDto) {
        try {
            String param = "enable_slot_log";
            String expectValue = "on";
            String actualValue = OpenGaussUtils.getVariableValue(param, verifyDto.getOpengaussConnection());
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
