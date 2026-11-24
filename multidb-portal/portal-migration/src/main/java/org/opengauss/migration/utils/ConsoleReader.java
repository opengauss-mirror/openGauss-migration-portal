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
import org.opengauss.migration.exceptions.MigrationException;

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
