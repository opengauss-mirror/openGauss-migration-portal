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

package org.opengauss.migration.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Thread utils
 *
 * @since 2025/4/21
 */
public class ThreadUtils {
    private static final Logger LOGGER = LogManager.getLogger(ThreadUtils.class);

    /**
     * Sleep for the specified time in milliseconds
     *
     * @param milliseconds sleep time in milliseconds
     */
    public static void sleep(long milliseconds) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("Sleep duration cannot be negative");
        }

        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread {} sleep is interrupted, error: {}", Thread.currentThread().getName(), e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
