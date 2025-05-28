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
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * ReversePermissionVerifyChain
 *
 * @since 1.1
 * @date :2023/11/3 15:22
 * @description: ReversePermissionVerifyChain
 * @version: 1.1
 */
public class ReversePermissionVerifyChain extends FullPermissionVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReversePermissionVerifyChain.class);

    private static final String[] PERMISSION_COLUMN = {
            Constants.PERMISSION_SELECT, Constants.PERMISSION_INSERT, Constants.PERMISSION_UPDATE,
            Constants.PERMISSION_DELETE
    };

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> databaseMap = new HashMap<>();
        resultMap.put("reverse_permission", databaseMap);
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
        if (pgConnection == null) {
            databaseMap.put(Constants.KEY_OPENGAUSS, Constants.CROSS_BAR);
        } else {
            int result;
            if (judgeSystemAdmin(pgConnection) || (hasReplicationRolePermission(pgConnection))) {
                result = Constants.KEY_FLAG_TRUE;
            } else {
                result = Constants.KEY_FLAG_FALSE;
            }
            databaseMap.put(Constants.KEY_OPENGAUSS, result);
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                    Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | result);
        }
    }

    private boolean hasReplicationRolePermission(PgConnection pgConnection) {
        boolean isOk = false;
        try {
            String permissionStr = JdbcUtils.selectStringValue(pgConnection,
                    "select rolreplication from pg_roles where rolname='"
                            + PortalControl.toolsMigrationParametersTable.get(Opengauss.USER) + "'", "rolreplication");
            LOGGER.info("rolreplication is {}, user is {}", permissionStr,
                    PortalControl.toolsMigrationParametersTable.get(Opengauss.USER));
            isOk = permissionStr.equals("1");
        } catch (SQLException e) {
            LOGGER.error("sql execute failed.");
        }
        return isOk;
    }
}
