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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * openGauss enable_subscription Verify Chain
 *
 * @since 2026/3/6
 */
public class OpenGaussEnableSubscriptionVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussEnableSubscriptionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss GUC Parameter enable_subscription Verify";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);
        doVerify(verifyDto, verifyResult);
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        Connection connection = verifyDto.getOpengaussConnection();
        try {
            if (!isEnableSubscriptionSupported(connection)) {
                return;
            }

            String param = "enable_subscription";
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
        addCurrentChainResult(verifyResult);
    }

    private boolean isEnableSubscriptionSupported(Connection connection) throws SQLException {
        String versionNumber = OpenGaussUtils.getOpenGaussVersion(connection);
        return versionNumber.compareTo("6.0.5") >= 0 && !versionNumber.equals("7.0.0-RC1")
                && !versionNumber.equals("7.0.0-RC2");
    }
}
