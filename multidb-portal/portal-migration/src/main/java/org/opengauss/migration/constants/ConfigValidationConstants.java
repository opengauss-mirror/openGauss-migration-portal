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

package org.opengauss.migration.constants;

import java.util.regex.Pattern;

/**
 * Config validation constants
 *
 * @since 2025/5/6
 */
public class ConfigValidationConstants {
    /**
     * Regular expression for IP address, including IPv4 and IPv6 formats
     */
    public static final Pattern IP_REGEX = Pattern.compile("((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})"
            + "(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}"
            + "|([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:)"
            + "|::([0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}");

    /**
     * Regular expression for port number
     */
    public static final Pattern PORT_REGEX = Pattern.compile("^("
            + "(102[4-9]|10[3-9]\\d|1[1-9]\\d{2}|[2-9]\\d{3}|"
            + "[1-5]\\d{4}|"
            + "6[0-4]\\d{3}|"
            + "655[0-2]\\d|"
            + "6553[0-5])"
            + ")$");

    private ConfigValidationConstants() {
    }
}
