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

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.migration.utils.MysqlUtils;
import org.opengauss.migration.utils.OpenGaussUtils;

import java.sql.SQLException;

/**
 * Mysql lower_case_table_names verify chain
 *
 * @since 2025/6/7
 */
public class MysqlLowerCaseVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlLowerCaseVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL lower_case_table_names Verify";

    @Override
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            String mysqlParamKey = "lower_case_table_names";
            String mysqlValue = MysqlUtils.getVariableValue(mysqlParamKey, verifyDto.getMysqlConnection());
            String openGaussParamKey = "dolphin.lower_case_table_names";
            String openGaussValue = OpenGaussUtils.getVariableValue(openGaussParamKey,
                    verifyDto.getOpengaussConnection());
            if (!mysqlValue.equals(openGaussValue)) {
                chainResult.setSuccess(false);
                chainResult.setDetail(String.format("Parameter lower_case_table_names has not the same value, "
                        + "MySQL value: %s, openGauss value: %s", mysqlValue, openGaussValue));
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
