/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * EnableSlotLogVerifyChain
 *
 * @date 2024/8/16 9:08
 * @since 0.0
 */
public class EnableSlotLogVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnableSlotLogVerifyChain.class);

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        if (Opengauss.isOpengaussClusterAvailable()) {
            doVerify(resultMap, pgConnection);
        }
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void doVerify(Map<String, Object> resultMap, PgConnection pgConnection) {
        Map<String, Object> paramMap = new HashMap<>();
        resultMap.put("enable_slot_log", paramMap);
        String enableSlotLog = "";
        if (pgConnection != null) {
            enableSlotLog = getEnableSlotLog(pgConnection);
        }

        boolean isOn = enableSlotLog.equals("on");
        if (isOn) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
        } else if (enableSlotLog.isEmpty()) {
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        } else if (enableSlotLog.equals("off")) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
            paramMap.put("expected_value", "on");
        } else {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message", enableSlotLog);
        }

        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isOn ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE));
    }

    private String getEnableSlotLog(PgConnection pgConnection) {
        String result;
        String selectSql = "show enable_slot_log;";
        try {
            result = JdbcUtils.selectStringValue(pgConnection, selectSql, "enable_slot_log");
            LOGGER.info("openGauss enable_slot_log is {}", result);
        } catch (SQLException e) {
            result = String.format("'%s' execute failed", selectSql);
            LOGGER.error(result, e);
        }
        return result;
    }
}
