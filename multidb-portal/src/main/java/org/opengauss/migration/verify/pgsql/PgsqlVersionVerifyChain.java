/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.pgsql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.PgsqlUtils;

import java.sql.SQLException;

/**
 * PostgreSQL version verify chain
 *
 * @since 2025/8/8
 */
public class PgsqlVersionVerifyChain extends AbstractPgsqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlVersionVerifyChain.class);
    private static final String VERIFY_NAME = "PostgreSQL Version Verify";

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);
        doVerify(verifyDto);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(VerifyDto verifyDto) {
        try {
            String actualVersion = PgsqlUtils.getPgsqlVersion(verifyDto.getSourceConnection());
            LOGGER.info("PostgreSQL version: {}", actualVersion);

            String[] versionParts = actualVersion.split("\\.");
            if (versionParts.length < 2) {
                chainResult.setSuccess(false);
                chainResult.setDetail("Failed to parse PostgreSQL major version and minor version, version: "
                        + actualVersion + ".");
                return;
            }

            int actualMajorVersion = Integer.parseInt(versionParts[0]);
            int expectMajorVersion = 9;
            if (actualMajorVersion > expectMajorVersion) {
                return;
            }

            int actualMinorVersion = Integer.parseInt(versionParts[1]);
            int expectMinorVersion = 4;
            if (actualMinorVersion > expectMinorVersion) {
                return;
            }

            if (versionParts.length >= 3) {
                int actualPatchVersion = Integer.parseInt(versionParts[2]);
                int expectPatchVersion = 26;
                if (actualPatchVersion >= expectPatchVersion) {
                    return;
                }
            }

            chainResult.setSuccess(false);
            chainResult.setDetail("PostgreSQL version is lower than 9.4.26, actual version: " + actualVersion);
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        } catch (NumberFormatException e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }
    }
}
