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
import org.opengauss.migration.utils.PgsqlUtils;

import java.sql.SQLException;

/**
 * PostgreSQL wal_level Verify Chain
 *
 * @since 2025/8/8
 */
public class PgsqlWalLevelVerifyChain extends AbstractPgsqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlWalLevelVerifyChain.class);
    private static final String VERIFY_NAME = "PostgreSQL Parameter wal_level Verify";

    @Override
    public void doVerify(PgsqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            String param = "wal_level";
            String expectValue = "logical";
            String actualValue = PgsqlUtils.getVariableValue(param, verifyDto.getPgsqlConnection());
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
        transfer(verifyDto, verifyResult);
    }
}
