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
import org.opengauss.portalcontroller.JdbcTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MigrationParameterVerifyChain
 *
 * @since 1.1
 * @date :2023/11/3 15:22
 * @description: MigrationParameterVerifyChain
 * @version: 1.1
 */
public class IncrementParameterVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementParameterVerifyChain.class);

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> paramMap = new HashMap<>();
        resultMap.put("increment_param", paramMap);
        verifyMysqlBinLogParam(resultMap, paramMap, mysqlConnection);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void verifyMysqlBinLogParam(Map<String, Object> resultMap, Map<String, Object> databaseMap,
        Connection mysqlConnection) {
        Map<String, Object> mysqMap = new HashMap<>();
        databaseMap.put(Constants.KEY_MYSQL, mysqMap);
        if (mysqlConnection != null) {
            List<String> errorPamramList = new ArrayList<>();
            judgeParam(mysqlConnection, errorPamramList, "log_bin", "ON");
            judgeParam(mysqlConnection, errorPamramList, "binlog_format", "ROW");
            judgeParam(mysqlConnection, errorPamramList, "binlog_row_image", "FULL");
            if (!errorPamramList.isEmpty()) {
                mysqMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                mysqMap.put("binlog_error", errorPamramList);
            } else {
                mysqMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            }
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | Integer.parseInt(
                    mysqMap.get(Constants.KEY_RESULT).toString()));
        } else {
            mysqMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        }
    }

    /**
     * judge param
     *
     * @param mysqlConnection mysql connection
     * @param errorParamList  error param list
     * @param key param name
     * @param value param value
     */
    public void judgeParam(Connection mysqlConnection, List<String> errorParamList, String key, String value) {
        String selectSql = "show variables like '" + key + "'";
        try {
            String permissionStr = JdbcTools.selectStringValue(mysqlConnection, selectSql, "Value");
            LOGGER.info("parameter {} is {}", key, permissionStr);
            if (!value.equals(permissionStr)) {
                errorParamList.add(key + "=" + permissionStr);
            }
        } catch (SQLException e) {
            errorParamList.add(selectSql + " execute exception");
            LOGGER.error(selectSql + " execute failed.", e);
        }
    }
}
