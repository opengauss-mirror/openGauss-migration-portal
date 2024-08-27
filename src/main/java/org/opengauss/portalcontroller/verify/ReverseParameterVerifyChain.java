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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MigrationParameterVerifyChain
 *
 * @date :2023/11/3 15:22
 * @description: MigrationParameterVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class ReverseParameterVerifyChain extends IncrementParameterVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReverseParameterVerifyChain.class);

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> paramMap = new HashMap<>();
        resultMap.put("reverse_param", paramMap);
        verifyOpenGaussBinLogParam(resultMap, paramMap, pgConnection);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void verifyOpenGaussBinLogParam(Map<String, Object> resultMap, Map<String, Object> databaseMap,
                                            PgConnection pgConnection) {
        Map<String, Object> openGaussMap = new HashMap<>();
        databaseMap.put(Constants.KEY_OPENGAUSS, openGaussMap);
        if (pgConnection != null) {
            Map<String, String> errorPamramMap = new HashMap<>();
            judgeParam(pgConnection, errorPamramMap, "wal_level", "logical");
            if (!errorPamramMap.isEmpty()) {
                openGaussMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                openGaussMap.put("binlog_error", "wal_level=" + errorPamramMap.get("wal_level"));
                String binlog = "wal_level=logical";
                openGaussMap.put("binlog", binlog);
                if (super.stringBuilder.length() != 0){
                    openGaussMap.put("SQLException", stringBuilder.toString());
                }
            } else {
                openGaussMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            }
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                    Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | Integer.parseInt(
                            openGaussMap.get(Constants.KEY_RESULT).toString()));
        } else {
            openGaussMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        }
    }

    @Override
    public void judgeParam(
            Connection opengaussConnection, Map<String, String> errorParamMap, String key, String value) {
        String selectSql = String.format(Constants.SHOW_OPENGAUSS_GUC_PARAM, key);
        try {
            String permissionStr = JdbcUtils.selectStringValue(opengaussConnection, selectSql, key);
            LOGGER.info("parameter {} is {}", key, permissionStr);
            if (!value.equals(permissionStr)) {
                errorParamMap.put(key, permissionStr);
            }
        } catch (SQLException e) {
            errorParamMap.put(key, selectSql + " execute exception");
            LOGGER.error(selectSql + " execute failed.", e);
            int index = e.getMessage().indexOf("ERROR");
            super.stringBuilder.append(e.getMessage().substring(index)).append(System.lineSeparator());
        }
    }
}
