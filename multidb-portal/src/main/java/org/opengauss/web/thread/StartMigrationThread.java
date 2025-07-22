/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.thread;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.handler.ThreadExceptionHandler;
import org.opengauss.web.service.TaskService;

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
