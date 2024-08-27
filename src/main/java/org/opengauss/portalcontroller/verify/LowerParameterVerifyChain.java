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
 * MigrationParameterVerifyChain
 *
 * @date :2023/11/3 15:22
 * @description: MigrationParameterVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class LowerParameterVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(LowerParameterVerifyChain.class);
    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> paramMap = new HashMap<>();
        resultMap.put("lower_param", paramMap);
        String mysqlLowerParam = "";
        String openGausslLowerParam = "";
        if (mysqlConnection == null) {
            paramMap.put(Constants.KEY_MYSQL, Constants.CROSS_BAR);
        } else {
            mysqlLowerParam = getMysqlLowParam(mysqlConnection, "lower_case_table_names");
        }
        if (pgConnection == null) {
            paramMap.put(Constants.KEY_OPENGAUSS, Constants.CROSS_BAR);
        } else {
            openGausslLowerParam = getOpenGausslLowParam(pgConnection);
        }
        boolean isSame = mysqlLowerParam.equals(openGausslLowerParam);
        if (isSame) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
        } else {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
            if (!"".equals(mysqlLowerParam)) {
                paramMap.put(Constants.KEY_MYSQL, "lower_case_table_names=" + mysqlLowerParam);
            }
            if (!"".equals(openGausslLowerParam)) {
                paramMap.put(Constants.KEY_OPENGAUSS, "lower_case_table_names=" + openGausslLowerParam);
            }
            if (stringBuilder.length()!=0){
                paramMap.put("SQLException", stringBuilder.toString());
            }
        }
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | (isSame
                        ? Constants.KEY_FLAG_TRUE
                        : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private String getMysqlLowParam(Connection mysqlConnection, String key) {
        String result;
        String selectSql = String.format(Constants.SHOW_MYSQL_SYSTEM_PARAM, key);
        try {
            result = JdbcUtils.selectStringValue(mysqlConnection, selectSql, "Value");
            LOGGER.info("mysql {} is {}", key, result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
            int index=e.getMessage().indexOf("ERROR");
            stringBuilder.append(e.getMessage().substring(index) + System.lineSeparator());
        }
        return result;
    }

    private String getOpenGausslLowParam(PgConnection pgConnection) {
        String result;
        String selectSql = String.format(Constants.SHOW_OPENGAUSS_GUC_PARAM, "dolphin.lower_case_table_names");
        try {
            result = JdbcUtils.selectStringValue(pgConnection, selectSql, "dolphin.lower_case_table_names");
            LOGGER.info("openGauss lower_case_table_names is {}", result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
            int index=e.getMessage().indexOf("ERROR");
            stringBuilder.append(e.getMessage().substring(index) + System.lineSeparator());
        }
        return result;
    }
}
