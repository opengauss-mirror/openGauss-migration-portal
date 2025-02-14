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

package org.opengauss.portalcontroller.thread;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.handler.ThreadExceptionHandler;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The type Thread get order.
 *
 * @author ：liutong
 * @since ：2022/12/24
 */
public class ThreadGetOrder extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadGetOrder.class);
    private static final int MAX_CACHE_SIZE = 20;
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final LinkedList<String> HISTORY_ORDERS = new LinkedList<>();

    /**
     * The Exit.
     */
    public boolean exit = false;

    private long lastedOrderTimestamp = 0L;

    /**
     * Read input order.
     */
    private void readInputOrder() {
        LOCK.lock();
        try {
            String path = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
            String fullLog = LogViewUtils.getFullLog(path);
            if (fullLog.isEmpty()) {
                return;
            }
            String[] strParts = fullLog.split(System.lineSeparator());
            String lastedInputCommand = strParts[0].trim();

            // check if the order is already processed
            Map<String, String> commandMap = FileUtils.parseOrderWithTimestamp(lastedInputCommand);
            long timestamp = parseInputCommandTimestamp(commandMap);
            if (HISTORY_ORDERS.contains(lastedInputCommand)) {
                return;
            }

            // check if the order timestamp is already processed
            if (timestamp <= lastedOrderTimestamp) {
                return;
            }
            LOGGER.info("read input order {}", lastedInputCommand);
            changeMigrationStatus(commandMap.get(Command.Parameters.ORDER));
            if (HISTORY_ORDERS.size() >= MAX_CACHE_SIZE) {
                HISTORY_ORDERS.remove(HISTORY_ORDERS.getLast());
            }
            HISTORY_ORDERS.addFirst(lastedInputCommand);
            lastedOrderTimestamp = timestamp;
        } finally {
            LOCK.unlock();
        }
    }

    private long parseInputCommandTimestamp(Map<String, String> commandMap) {
        try {
            return Long.parseLong(commandMap.get(Parameter.ORDER_INVOKED_TIMESTAMP));
        } catch (NumberFormatException ex) {
            LOGGER.error("parse input order timestamp error {}", commandMap);
        }
        return 0L;
    }

    /**
     * Change migration status.
     *
     * @param command the command
     */
    private void changeMigrationStatus(String command) {
        switch (command) {
            case Command.Stop.INCREMENTAL_MIGRATION: {
                Plan.stopIncrementalMigration = true;
                Plan.runIncrementalMigration = false;
                break;
            }
            case Command.Stop.REVERSE_MIGRATION: {
                Plan.stopReverseMigration = true;
                Plan.runReverseMigration = false;
                break;
            }
            case Command.Run.INCREMENTAL_MIGRATION: {
                Plan.runIncrementalMigration = true;
                Plan.stopIncrementalMigration = false;
                break;
            }
            case Command.Run.INCREMENTAL_MIGRATION_SOURCE: {
                Plan.runIncrementalMigrationEndpoint = Method.Name.CONNECT_SOURCE;
                LOGGER.info("set input order to plan.execPlan incremental = {}", Plan.runIncrementalMigrationEndpoint);
                break;
            }
            case Command.Run.INCREMENTAL_MIGRATION_SINK: {
                Plan.runIncrementalMigrationEndpoint = Method.Name.CONNECT_SINK;
                LOGGER.info("set input order to plan.execPlan incremental = {}", Plan.runIncrementalMigrationEndpoint);
                break;
            }
            case Command.Run.REVERSE_MIGRATION: {
                Plan.runReverseMigration = true;
                Plan.stopReverseMigration = false;
                break;
            }
            case Command.Run.REVERSE_MIGRATION_SOURCE: {
                Plan.runReverseMigrationEndpoint = Method.Name.REVERSE_CONNECT_SOURCE;
                LOGGER.info("set input order to plan.execPlan reverse = {}", Plan.runReverseMigrationEndpoint);
                break;
            }
            case Command.Run.REVERSE_MIGRATION_SINK: {
                Plan.runReverseMigrationEndpoint = Method.Name.REVERSE_CONNECT_SINK;
                LOGGER.info("set input order to plan.execPlan reverse = {}", Plan.runReverseMigrationEndpoint);
                break;
            }
            case Command.Stop.PLAN: {
                Plan.stopPlan = true;
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        while (!exit && !Plan.stopPlan) {
            readInputOrder();
            ProcessUtils.sleepThread(1000, "getting order");
        }
    }
}
