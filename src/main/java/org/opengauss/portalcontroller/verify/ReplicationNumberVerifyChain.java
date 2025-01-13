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
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.command.mysql.VerifyCommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * ReplicationNumberVerifyChain
 *
 * @since 1.1
 * @date :2024/01/27 15:25
 * @description: ReplicationNumberVerifyChain
 * @version: 1.1
 */
public class ReplicationNumberVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationNumberVerifyChain.class);
    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> numberMap = new HashMap<>();
        resultMap.put("replication_slots", numberMap);
        String replicationSlots = "0";
        String maxReplication = "-1";
        try {
            if (pgConnection == null) {
                numberMap.put(Constants.KEY_OPENGAUSS, Constants.CROSS_BAR);
            } else if (VerifyCommandReceiver.isReverseVerify() && isReplicationSlotExists(pgConnection)) {
                numberMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
                super.transfer(resultMap, mysqlConnection, pgConnection);
                return;
            } else {
                replicationSlots = getReplicationNumber(pgConnection);
                maxReplication = getMaxReplicationNumber(pgConnection);
            }
            int replicationNumber = Integer.parseInt(replicationSlots);
            int maxReplicationNumber = Integer.parseInt(maxReplication);
            boolean isOverMaxReplicationSlots = replicationNumber < maxReplicationNumber;
            if (isOverMaxReplicationSlots) {
                numberMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            } else {
                numberMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                if (!replicationSlots.isEmpty()) {
                    numberMap.put("replication_number", replicationSlots);
                }
            }
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                    Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                            | (isOverMaxReplicationSlots ? Constants.KEY_FLAG_TRUE
                            : Constants.KEY_FLAG_FALSE));
            super.transfer(resultMap, mysqlConnection, pgConnection);
        } catch (NumberFormatException e) {
            LOGGER.error("replication number verify error: ", e);
        }
    }

    private boolean isReplicationSlotExists(PgConnection pgConnection) {
        String slotName = "slot_" + PortalControl.commandLineParameterStringMap.get(Command.Parameters.ID);
        try (Statement statement = pgConnection.createStatement()) {
            String selectSlotSql = "SELECT * FROM pg_get_replication_slots()";
            String columnName = "slot_name";
            return JdbcUtils.isSpecifiedNameExist(statement, selectSlotSql, slotName, columnName);
        } catch (SQLException e) {
            LOGGER.error("{}Failed to check whether the '{}' replication slot exists failed.",
                    ErrorCode.SQL_EXCEPTION, slotName, e);
        }
        return false;
    }

    private String getReplicationNumber(PgConnection pgConnection) {
        String result;
        String selectSql = "select count(*) from pg_get_replication_slots();";
        try {
            result = JdbcUtils.selectStringValue(pgConnection, selectSql, "count");
            LOGGER.info("pg_get_replication_slots number is {}", result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
        }
        return result;
    }

    private String getMaxReplicationNumber(PgConnection pgConnection) {
        String result;
        String selectSql = String.format(Constants.SHOW_OPENGAUSS_GUC_PARAM, "max_replication_slots");
        try {
            result = JdbcUtils.selectStringValue(pgConnection, selectSql, "max_replication_slots");
            LOGGER.info("max_replication_slots number is {}", result);
        } catch (SQLException e) {
            result = selectSql + " execute failed";
            LOGGER.error(result, e);
        }
        return result;
    }
}
