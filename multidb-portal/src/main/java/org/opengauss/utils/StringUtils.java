/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

/**
 * String utils
 *
 * @since 2025/4/24
 */
public class StringUtils {
    /**
     * Check if string is null or blank
     *
     * @param str string
     * @return true if string is null or blank, otherwise false
     */
    public static boolean isNullOrBlank(String str) {
        return str == null || str.isBlank();
    }
}
