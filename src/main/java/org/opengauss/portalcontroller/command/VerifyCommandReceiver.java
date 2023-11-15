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

package org.opengauss.portalcontroller.command;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.JdbcTools;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.verify.Constants;
import org.opengauss.portalcontroller.verify.VerifyChainBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * VerifyCommandReceiver
 *
 * @since 1.1
 * @date :2023/11/3 10:22
 * @description: VerifyCommandReceiver
 * @version: 1.1
 */
public class VerifyCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyCommandReceiver.class);

    /**
     * verify before migration
     *
     * @param order the order
     */
    public void action(String order) {
        LOGGER.info("start execute command={}", order);
        Map<String, Object> resultMap = new HashMap<>();
        PgConnection pgConnection = null;
        Connection mysqlConnection = null;
        try {
            mysqlConnection = JdbcTools.getMysqlConnection();
            pgConnection = JdbcTools.getPgConnection();
            LOGGER.info("migration_mode is {}", System.getProperty("migration_mode"));
            if (Command.Verify.VERIFY_PRE_MIGRATION.equals(order)) {
                // 2->online,1->offline
                if (Constants.MIGRATION_MODE_OFFLINE.equals(
                    System.getProperty("migration_mode", Constants.MIGRATION_MODE_ONLINE))) {
                    VerifyChainBuilder.getOfflineVerifyChain().verify(resultMap, mysqlConnection, pgConnection);
                } else {
                    VerifyChainBuilder.getOnlineVerifyChain().verify(resultMap, mysqlConnection, pgConnection);
                }
            } else if (Command.Verify.VERIFY_REVERSE_MIGRATION.equals(order)) {
                VerifyChainBuilder.getReverseVerifyChain().verify(resultMap, mysqlConnection, pgConnection);
            } else {
                Tools.outputResult(false, order);
            }
        } finally {
            JdbcTools.closeConnection(mysqlConnection);
            JdbcTools.closeConnection(pgConnection);
        }
        // base on verify result,output information flag
        int flag = Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString());
        if (flag == Constants.KEY_FLAG_TRUE) {
            Tools.outputResult(true, "verify migration success.");
        } else {
            // write json to file
            Tools.writeJsonToFile(resultMap);
            Tools.outputResult(false, "verify migration failed.");
        }
        LOGGER.info("execute command={} end", order);
    }
}
