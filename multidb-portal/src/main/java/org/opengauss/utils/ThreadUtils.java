/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

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
