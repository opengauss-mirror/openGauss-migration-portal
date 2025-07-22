/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.pgsql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.OpenGaussUtils;

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
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        try {
            Connection sourceConnection = verifyDto.getSourceConnection();
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
