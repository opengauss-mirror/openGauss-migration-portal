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

package org.opengauss.portalcontroller.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Input reader
 *
 * @since 2025/07/30
 */
public class InputReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(InputReader.class);
    private static final Scanner SCANNER = new Scanner(System.in);

    /**
     * Read system input
     *
     * @param message message
     * @return input line
     */
    public static String readLine(String message) {
        LOGGER.info(message);
        return SCANNER.nextLine();
    }

    /**
     * Close the scanner
     */
    public static void close() {
        SCANNER.close();
    }
}
