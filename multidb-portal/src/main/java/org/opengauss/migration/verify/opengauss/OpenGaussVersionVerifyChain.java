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

import java.sql.SQLException;

/**
 * OpenGaussVersionVerifyChain
 *
 * @since 2025/11/1
 */
public class OpenGaussVersionVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussVersionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Version Verify";

    private final String minimumVersion;

    public OpenGaussVersionVerifyChain(String minimumVersion) {
        this.minimumVersion = minimumVersion;
    }

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            String version = OpenGaussUtils.getOpenGaussVersion(verifyDto.getOpengaussConnection());
            LOGGER.info("openGauss version: {}", version);

            if (compareVersions(version, minimumVersion) < 0) {
                chainResult.setSuccess(false);
                chainResult.setDetail(
                        "openGauss version " + version + " is lower than minimum version " + minimumVersion + ".");
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
