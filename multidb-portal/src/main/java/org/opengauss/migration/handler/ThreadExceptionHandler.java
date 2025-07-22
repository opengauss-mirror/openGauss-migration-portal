/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opengauss.Main;
import org.opengauss.migration.MigrationManager;

/**
 * Migration thread uncaught exception handler
 *
 * @since 2025/4/1
 */
public class ThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LogManager.getLogger(ThreadExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable throwable) {
        LOGGER.error("Thread {} occur exception: ", t.getName(), throwable);

        try {
            MigrationManager.getInstance().stopOnError();
        } finally {
            Main.stopQuarkus();
        }
    }
}
