/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

/**
 * time utils
 *
 * @since 2025/5/12
 */
public class TimeUtils {
    /**
     * Get current time in milliseconds
     *
     * @return current time in milliseconds
     */
    public static long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Get timestamp from 2025-01-01
     *
     * @return timestamp from 2025-01-01
     */
    public static long timestampFrom20250101() {
        ThreadUtils.sleep(100);
        long timestampOf20250101 = 1735660800000L;
        return System.currentTimeMillis() - timestampOf20250101;
    }
}
