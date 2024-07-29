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
 * DatabaseEncryptionVerifyChain
 *
 * @since 1.1
 * @date :2024/01/27 15:25
 * @description: DatabaseEncryptionVerifyChain
 * @version: 1.1
 */
public class DatabaseEncryptionVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseEncryptionVerifyChain.class);
    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> encryptionMap = new HashMap<>();
        resultMap.put("mysql_encryption", encryptionMap);
        String mysqlEncryption = "";
        if (mysqlConnection == null) {
            encryptionMap.put(Constants.KEY_MYSQL, Constants.CROSS_BAR);
        } else {
            boolean checkEncryption = checkMysqlVersion(mysqlConnection);
            if (checkEncryption) {
                mysqlEncryption = getMysqlEncryption(mysqlConnection);
            } else {
                mysqlEncryption = Constants.ENCRYPTION_MYSQL;
            }
        }
        boolean isSame = mysqlEncryption.equals(Constants.ENCRYPTION_MYSQL);
        if (isSame) {
            encryptionMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
        } else {
            encryptionMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
            if (!mysqlEncryption.isEmpty()) {
                encryptionMap.put("encryption", mysqlEncryption);
                encryptionMap.put("valid_encryption", Constants.ENCRYPTION_MYSQL);
            }
        }
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | (isSame
                        ? Constants.KEY_FLAG_TRUE
                        : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private String getMysqlEncryption(Connection mysqlConnection) {
        String result;
        String selectSql = "select @@default_authentication_plugin;";
        try {
            result = JdbcUtils.selectStringValue(mysqlConnection, selectSql, "@@default_authentication_plugin");
            LOGGER.info("mysql @@default_authentication_plugin is {}", result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
        }
        return result;
    }

    /**
     * Verify encryption methods for versions 8 and above, because the default
     * encryption method for version 8 does not support migration
     */
    private boolean checkMysqlVersion(Connection mysqlConnection) {
        String result;
        String selectSql = "select version();";
        try {
            result = JdbcUtils.selectStringValue(mysqlConnection, selectSql, "version()");
            LOGGER.info("mysql version is {}", result);
            if (result.startsWith("8")) {
                return true;
            }
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
        }
        return false;
    }
}
