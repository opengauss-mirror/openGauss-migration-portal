/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.command.mysql;

import lombok.Getter;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.command.CommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
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
 * @date :2023/11/3 10:22
 * @description: VerifyCommandReceiver
 * @version: 1.1
 * @since 1.1
 */
public class VerifyCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyCommandReceiver.class);

    @Getter
    private static boolean isReverseVerify = false;

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
            mysqlConnection = JdbcUtils.getMysqlConnection();
            pgConnection = JdbcUtils.getPgConnection();
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
                isReverseVerify = true;
                VerifyChainBuilder.getReverseVerifyChain().verify(resultMap, mysqlConnection, pgConnection);
            } else {
                LogViewUtils.outputResult(false, order);
            }
        } finally {
            JdbcUtils.closeConnection(mysqlConnection);
            JdbcUtils.closeConnection(pgConnection);
        }
        // base on verify result,output information flag
        int flag = Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString());
        if (flag == Constants.KEY_FLAG_TRUE) {
            LogViewUtils.outputResult(true, "verify migration success.");
        } else {
            // write json to file
            ParamsUtils.writeJsonToFile(resultMap);
            LogViewUtils.outputResult(false, "verify migration failed.");
        }
        LOGGER.info("execute command={} end", order);
    }
}
