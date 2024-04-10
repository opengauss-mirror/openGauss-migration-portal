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

import java.sql.Connection;
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
            Map<String, String> errorPamramList = new HashMap<>();
            super.judgeParam(pgConnection, errorPamramList, "wal_level", "logical");
            if (!errorPamramList.isEmpty()) {
                openGaussMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                openGaussMap.put("binlog_error", errorPamramList);
                String binlog = "wal_level=logical";
                openGaussMap.put("binlog", binlog);
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
}
