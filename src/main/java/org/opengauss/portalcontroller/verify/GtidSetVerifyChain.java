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

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL Executed_Gtid_Set verify
 *
 * @since 2025/5/26
 */
public class GtidSetVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(GtidSetVerifyChain.class);

    private HashMap<String, Object> paramMap = new HashMap<>();

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        boolean isVerified = false;

        if (mysqlConnection == null) {
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        } else if (isExecutedGtidSetAvailable(mysqlConnection)) {
            isVerified = true;
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
        } else {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
        }

        String verifyParamKey = "gtid_set";
        resultMap.put(verifyParamKey, paramMap);

        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isVerified ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private boolean isExecutedGtidSetAvailable(Connection mysqlConnection) {
        String sql = "SHOW MASTER STATUS;";
        try {
            String gtidSet = JdbcUtils.selectStringValue(mysqlConnection, sql, "Executed_Gtid_Set");
            if (gtidSet != null && gtidSet.contains(":1-")) {
                return true;
            }
        } catch (SQLException e) {
            Map<String, String> binlogPosMap = JdbcUtils.selectMapValue(mysqlConnection, sql,
                new String[] {"File", "Position"});
            return binlogPosMap.get("File") != null && binlogPosMap.get("Position") != null;
        }
        LOGGER.error("Failed to execute Gtid Set and binlog position query");
        return false;
    }
}
