/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.quarkus.thread;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.handler.ThreadExceptionHandler;
import org.opengauss.quarkus.service.TaskService;

/**
 * Start migration thread
 *
 * @since 2025/3/26
 */
@Named("startMigrationThread")
@ApplicationScoped
public class StartMigrationThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(StartMigrationThread.class);

    @Inject
    @Named("taskService")
    TaskService taskService;

    public StartMigrationThread() {
        super("Migration-Main-Thread");
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        LOGGER.info("Starting migration...");
        taskService.startTask();
    }
}
