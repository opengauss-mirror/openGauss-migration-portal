/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

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
