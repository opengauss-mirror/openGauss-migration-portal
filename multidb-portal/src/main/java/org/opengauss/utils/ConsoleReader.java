/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.exceptions.MigrationException;

import java.io.Console;
import java.io.IOException;

/**
 * ConsoleReader
 *
 * @since 2025/9/15
 */
public class ConsoleReader {
    private static final Logger LOGGER = LogManager.getLogger(ConsoleReader.class);

    /**
     * Read password from console
     *
     * @param prompt prompt
     * @return password
     */
    public static String readPassword(String prompt) {
        Console console = System.console();
        if (console != null) {
            return String.valueOf(console.readPassword(prompt));
        }

        LOGGER.warn("System.console() is null, using System.in to read password");
        return readVisiblePassword(prompt);
    }

    private static String readVisiblePassword(String prompt) {
        try {
            LOGGER.info(prompt);
            StringBuilder password = new StringBuilder();
            int ch;
            while ((ch = System.in.read()) != -1) {
                if (ch == '\n' || ch == '\r') {
                    break;
                }
                password.append((char) ch);
            }
            return password.toString();
        } catch (IOException e) {
            throw new MigrationException("Failed to read password from System.in", e);
        }
    }
}
