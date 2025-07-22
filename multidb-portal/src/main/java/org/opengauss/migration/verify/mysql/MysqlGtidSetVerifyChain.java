/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MysqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mysql gtid set verify chain
 *
 * @since 2025/6/7
 */
public class MysqlGtidSetVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlGtidSetVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Executed_Gtid_Set Verify";

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        try {
            Connection connection = verifyDto.getSourceConnection();
            String executedGtidSet = MysqlUtils.getExecutedGtidSet(connection);
            if (executedGtidSet != null && executedGtidSet.contains(":1-")) {
                chainResult.setSuccess(true);
            } else {
                chainResult.setSuccess(false);
                chainResult.setDetail("Executed_Gtid_Set is empty or the number of transactions is 1");
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
