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
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Mysql;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * IncrementPermissionVerifyChain
 *
 * @date :2023/11/3 15:22
 * @description: IncrementPermissionVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class IncrementPermissionVerifyChain extends FullPermissionVerifyChain {
    private static final String[] PERMISSION_COLUMN = {
            Constants.PERMISSION_SELECT, Constants.PERMISSION_REP_SLAVE, Constants.PERMISSION_REP_CLIENT
    };

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> databaseMap = new HashMap<>();
        resultMap.put("increment_permission", databaseMap);
        verifyMysqlPermission(resultMap, databaseMap, mysqlConnection);
        verifyOpenGaussPermission(resultMap, databaseMap, pgConnection);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void verifyMysqlPermission(Map<String, Object> resultMap, Map<String, Object> databaseMap,
                                       Connection mysqlConnection) {
        super.verifyMysqlPermission(resultMap, databaseMap, mysqlConnection,
                new StringBuilder("select ").append(String.join(",", PERMISSION_COLUMN))
                        .append(" from mysql.user where user='")
                        .append(PortalControl.toolsMigrationParametersTable.get(Mysql.USER))
                        .append("';")
                        .toString(), PERMISSION_COLUMN);
    }

    private void verifyOpenGaussPermission(Map<String, Object> resultMap, Map<String, Object> databaseMap,
                                           PgConnection pgConnection) {
        super.verifyOpenGaussPermission(resultMap, databaseMap, pgConnection,
                new String[]{"C", "T", "c", "A", "P", "m"});
    }
}
