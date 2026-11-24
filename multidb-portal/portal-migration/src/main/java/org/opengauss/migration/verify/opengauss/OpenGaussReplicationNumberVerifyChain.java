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
 * openGauss replication number verify chain
 *
 * @since 2025/6/7
 */
public class OpenGaussReplicationNumberVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussReplicationNumberVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Number Of Remaining Replication Slots Verify";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            Connection targetConnection = verifyDto.getOpengaussConnection();
            int countNumbers = OpenGaussUtils.countReplicationSlots(targetConnection);
            String maxNumbers = OpenGaussUtils.getVariableValue("max_replication_slots", targetConnection);
            if (countNumbers == Integer.parseInt(maxNumbers)) {
                LOGGER.error("Number of remaining replication slots is 0, current number of replication slots is {},"
                        + " max number of replication slots is {}.", countNumbers, maxNumbers);
                chainResult.setSuccess(false);
                chainResult.setDetail("Number of remaining replication slots is 0");
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
