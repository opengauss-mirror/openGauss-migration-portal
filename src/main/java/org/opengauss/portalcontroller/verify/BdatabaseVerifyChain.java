/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
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
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * BdatabaseVerifyChain
 *
 * @since 1.1
 * @date :2024/01/27 15:25
 * @description: BdatabaseVerifyChain
 * @version: 1.1
 */
public class BdatabaseVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(BdatabaseVerifyChain.class);
    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> compatibilityMap = new HashMap<>();
        resultMap.put("sql_compatibility", compatibilityMap);
        String openGaussCompatibility = "";
        if (pgConnection == null) {
            compatibilityMap.put(Constants.KEY_OPENGAUSS, Constants.CROSS_BAR);
        } else {
            openGaussCompatibility = getOpenGaussCompatibility(pgConnection);
        }
        boolean isBCompatibility = openGaussCompatibility.equals("B");
        if (isBCompatibility) {
            compatibilityMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
        } else {
            compatibilityMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
            if (!openGaussCompatibility.isEmpty()) {
                compatibilityMap.put("sql_compatibility", openGaussCompatibility);
                compatibilityMap.put("valid_sql_compatibility", "B");
            }
        }
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isBCompatibility
                        ? Constants.KEY_FLAG_TRUE
                        : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private String getOpenGaussCompatibility(PgConnection pgConnection) {
        String result;
        String selectSql = "show sql_compatibility;";
        try {
            result = JdbcUtils.selectStringValue(pgConnection, selectSql, "sql_compatibility");
            LOGGER.info("openGauss sql_compatibility is {}", result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
        }
        return result;
    }
}
