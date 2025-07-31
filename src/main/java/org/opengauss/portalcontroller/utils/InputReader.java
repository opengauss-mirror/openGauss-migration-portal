/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
