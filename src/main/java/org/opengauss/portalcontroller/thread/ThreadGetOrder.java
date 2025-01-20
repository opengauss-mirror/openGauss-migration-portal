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

/**
 * The type Thread get order.
 */
public class ThreadGetOrder extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadGetOrder.class);
    /**
     * The Exit.
     */
    public boolean exit = false;

    /**
     * Read input order.
     */
    public static void readInputOrder() {
        String path = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        String fullLog = LogViewUtils.getFullLog(path);
        if (!fullLog.isEmpty()) {
            String[] strParts = fullLog.split(System.lineSeparator());
            String order = FileUtils.parseOrderWithTimestamp(strParts[0].trim()).get(Command.Parameters.ORDER);

            if (!PortalControl.latestCommand.equals(order)) {
                LOGGER.info("read input order {}", order);
                PortalControl.latestCommand = order;
                changeMigrationStatus(order);
            }
        }
    }

    /**
     * Change migration status.
     *
     * @param command the command
     */
    public static void changeMigrationStatus(String command) {
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
