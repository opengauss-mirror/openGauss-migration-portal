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

import com.alibaba.fastjson.JSONObject;
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
 * FullPermissionVerifyChain
 *
 * @date :2023/11/3 15:22
 * @description: FullPermissionVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class FullPermissionVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(FullPermissionVerifyChain.class);

    private static final String[] PERMISSION_COLUMN = {
            Constants.PERMISSION_SELECT, Constants.PERMISSION_RELOAD, Constants.PERMISSION_REP_CLIENT,
            Constants.PERMISSION_LOCK_TABLES
    };

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> databaseMap = new HashMap<>();
        resultMap.put("full_permission", databaseMap);
        verifyMysqlPermission(resultMap, databaseMap, mysqlConnection,
                new StringBuilder("select ").append(String.join(",", PERMISSION_COLUMN))
                        .append(" from mysql.user where user='")
                        .append(PortalControl.toolsMigrationParametersTable.get(Mysql.USER))
                        .append("';")
                        .toString(), PERMISSION_COLUMN);
        verifyOpenGaussPermission(resultMap, databaseMap, pgConnection, new String[]{"C"});
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    /**
     * verify mysql full migration permission
     *
     * @param resultMap         resultMap
     * @param databaseMap       databaseMap
     * @param mysqlConnection   mysqlConnection
     * @param sql               sql content
     * @param permissionColumns permission column
     */
    protected void verifyMysqlPermission(Map<String, Object> resultMap, Map<String, Object> databaseMap,
                                         Connection mysqlConnection, String sql, String[] permissionColumns) {
        if (mysqlConnection == null) {
            // not connect default "-"
            databaseMap.put(Constants.KEY_MYSQL, Constants.CROSS_BAR);
        } else {
            Map<String, String> permissionMap = JdbcUtils.selectMapValue(mysqlConnection, sql, permissionColumns);
            boolean isMet = true;
            for (String permissionColumn : permissionColumns) {
                if (!Constants.PERMISSION_YES.equals(permissionMap.get(permissionColumn))) {
                    isMet = false;
                }
            }
            LOGGER.info("permissionMap is {}", JSONObject.toJSON(permissionMap));
            int result = isMet ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE;
            databaseMap.put(Constants.KEY_MYSQL, result);
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                    Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | result);
        }
    }

    /**
     * verify openGauss full migration permission
     *
     * @param resultMap       resultMap
     * @param databaseMap     databaseMap
     * @param pgConnection    pgConnection
     * @param needPermissions needPermissions
     */
    protected void verifyOpenGaussPermission(Map<String, Object> resultMap, Map<String, Object> databaseMap,
                                             PgConnection pgConnection, String[] needPermissions) {
        if (pgConnection == null) {
            databaseMap.put(Constants.KEY_OPENGAUSS, Constants.CROSS_BAR);
            return;
        }
        int result;
        if (judgeSystemAdmin(pgConnection) || hasPermission(pgConnection, needPermissions)) {
            result = Constants.KEY_FLAG_TRUE;
        } else {
            result = Constants.KEY_FLAG_FALSE;
        }
        databaseMap.put(Constants.KEY_OPENGAUSS, result);
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | result);
    }

    /**
     * judge user is sysadmin?
     *
     * @param pgConnection pgConnection
     * @return is sysadmin or not
     */
    protected boolean judgeSystemAdmin(PgConnection pgConnection) {
        boolean isAdmin = false;
        try {
            String permissionStr = JdbcUtils.selectStringValue(pgConnection,
                    "select rolsystemadmin from pg_roles where rolname= '"
                            + PortalControl.toolsMigrationParametersTable.get(Opengauss.USER) + "'", "rolsystemadmin");
            LOGGER.info("permissionStr is {}", permissionStr);
            isAdmin = permissionStr.equals("1") || permissionStr.equals("t");
        } catch (SQLException e) {
            LOGGER.error("select rolsystemadmin from pg_roles where rolname= '"
                    + PortalControl.toolsMigrationParametersTable.get(Opengauss.USER) + "' execute failed.");
        }
        return isAdmin;
    }

    private boolean hasPermission(PgConnection pgConnection, String[] needPermissions) {
        boolean hasPermission = true;
        try {
            String permissionStr = JdbcUtils.selectStringValue(pgConnection,
                    "select datacl from pg_database where datname= '"
                            + PortalControl.toolsMigrationParametersTable.get(Opengauss.DATABASE_NAME) + "'", "datacl");
            LOGGER.info("permissionStr is {}", permissionStr);
            StringBuilder permissionBuild = new StringBuilder();
            if (permissionStr == null) {
                return false;
            }
            if (permissionStr.contains(",")) {
                String[] userPermissions = permissionStr.split(",");
                for (String userPermission : userPermissions) {
                    String user = userPermission.split("=")[0];
                    String permission = userPermission.split("=")[1];
                    if (PortalControl.toolsMigrationParametersTable.get(Opengauss.USER).equals(user)) {
                        permissionBuild.append(permission);
                    }
                }
            }
            for (String needPermission : needPermissions) {
                if (!permissionBuild.toString().contains(needPermission)) {
                    hasPermission = false;
                    break;
                }
            }
        } catch (SQLException e) {
            hasPermission = false;
            LOGGER.error("sql execute failed.");
        }
        return hasPermission;
    }
}
