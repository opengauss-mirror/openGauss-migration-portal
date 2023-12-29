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
import java.util.HashMap;
import java.util.Map;

/**
 * DatabaseConnectVerifyChain
 *
 * @date :2023/11/3 15:25
 * @description: DatabaseConnectVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class DatabaseConnectVerifyChain extends AbstractPreMigrationVerifyChain {
    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> databaseMap = new HashMap<>();
        resultMap.put("database_connect", databaseMap);
        int mysqlResult = (mysqlConnection != null) ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE;
        databaseMap.put(Constants.KEY_MYSQL, mysqlResult);
        int pgResult = (pgConnection != null) ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE;
        databaseMap.put(Constants.KEY_OPENGAUSS, pgResult);
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | mysqlResult | pgResult);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }
}
