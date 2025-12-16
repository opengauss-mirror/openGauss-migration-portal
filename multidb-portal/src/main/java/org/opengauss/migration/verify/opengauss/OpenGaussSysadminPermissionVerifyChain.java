/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.OpenGaussUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * OpenGaussSysadminPermissionVerifyChain
 *
 * @since 2025/10/20
 */
public class OpenGaussSysadminPermissionVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussSysadminPermissionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Connect User Sysadmin Permission Verify";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        Connection connection = verifyDto.getOpengaussConnection();
        String username = verifyDto.getMigrationConfigDto().getOpenGaussConnectInfo().getUsername();
        try {
            boolean isSysadmin = OpenGaussUtils.isSystemAdmin(username, connection);
            if (!isSysadmin) {
                chainResult.setSuccess(false);
                chainResult.setDetail("User " + username + " is not sysadmin");
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
