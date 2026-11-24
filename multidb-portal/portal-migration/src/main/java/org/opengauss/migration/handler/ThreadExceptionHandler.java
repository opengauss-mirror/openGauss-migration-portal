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

package org.opengauss.migration.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
            try {
                MigrationManager.getInstance().fireExit();
            } catch (Exception e) {
                LOGGER.warn("Failed to fire migration exit", e);
            }
        }
    }
}
