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
 * openGauss reverse permission verify chain
 *
 * @since 2025/6/7
 */
public class OpenGaussReversePermissionVerifyChain extends OpenGaussFullPermissionVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussReversePermissionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Connect User Reverse Migration Permission Verify";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            if (!isSystemAdmin(verifyDto)) {
                boolean isReplicationRole = OpenGaussUtils.isReplicationRole(
                        verifyDto.getMigrationConfigDto().getOpenGaussConnectInfo().getUsername(),
                        verifyDto.getOpengaussConnection());
                if (!isReplicationRole) {
                    chainResult.setSuccess(false);
                    chainResult.setDetail("The user does not have the replication role");
                }
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
