/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

import org.opengauss.PGProperty;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

/**
 * replication connection verify
 *
 * @since 2024-10-11
 */
public class ReplicationConnectVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationConnectVerifyChain.class);

    private final HashMap<Object, Object> paramMap = new HashMap<>();

    private String errorMessage = "";

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        String verifyParamKey = "hba_conf";
        resultMap.put(verifyParamKey, paramMap);

        boolean isValid = false;
        if (pgConnection == null) {
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        } else if (canCreateReplicationConnection()) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            isValid = true;
        } else {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message", errorMessage);
        }

        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isValid ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private boolean canCreateReplicationConnection() {
        Hashtable<String, String> migrationParametersTable = PortalControl.toolsMigrationParametersTable;

        Properties properties = new Properties();
        PGProperty.USER.set(properties, migrationParametersTable.get(Opengauss.USER));
        PGProperty.PASSWORD.set(properties, migrationParametersTable.get(Opengauss.PASSWORD));
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");

        String ip = migrationParametersTable.get(Opengauss.DATABASE_HOST);
        String database = migrationParametersTable.get(Opengauss.DATABASE_NAME);
        int port = Integer.parseInt(migrationParametersTable.get(Opengauss.DATABASE_PORT));
        int haPort = port + 1;
        String urlModel = "jdbc:opengauss://%s:%d/%s";
        String url = String.format(urlModel, ip, port, database);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            return true;
        } catch (SQLException e) {
            url = String.format(urlModel, ip, haPort, database);
            try (Connection connection = DriverManager.getConnection(url, properties)) {
                return true;
            } catch (SQLException ex) {
                LOGGER.error("Failed to create replication connection", ex);
                errorMessage = ex.getMessage();
            }
        }
        return false;
    }
}
