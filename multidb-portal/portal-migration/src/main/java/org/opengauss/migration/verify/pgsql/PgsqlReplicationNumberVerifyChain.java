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

package org.opengauss.migration.verify.pgsql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.PgsqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.migration.utils.OpenGaussUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * PostgreSQL replication number verify chain
 *
 * @since 2027/7/9
 */
public class PgsqlReplicationNumberVerifyChain extends AbstractPgsqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlReplicationNumberVerifyChain.class);
    private static final String VERIFY_NAME = "PostgreSQL Number Of Remaining Replication Slots Verify";

    @Override
    public void doVerify(PgsqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            Connection sourceConnection = verifyDto.getPgsqlConnection();
            int countNumbers = OpenGaussUtils.countReplicationSlots(sourceConnection);
            String maxNumbers = OpenGaussUtils.getVariableValue("max_replication_slots", sourceConnection);
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
