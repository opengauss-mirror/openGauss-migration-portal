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
