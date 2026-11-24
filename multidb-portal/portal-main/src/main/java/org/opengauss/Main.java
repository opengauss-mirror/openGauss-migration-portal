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

package org.opengauss;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.Command;
import org.opengauss.command.CommandFactory;
import org.opengauss.command.HelpCommand;
import org.opengauss.command.parser.CommandParser;
import org.opengauss.handler.PortalExceptionHandler;

/**
 * Main class
 *
 * @since 2025/2/27
 */
@QuarkusMain
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    private static String[] args;

    /**
     * Main method
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Thread.currentThread().setUncaughtExceptionHandler(new PortalExceptionHandler());
        Main.args = args;
        Command command = parseCommand(args);
        if (command != null) {
            command.execute();
        }
    }

    /**
     * Start quarkus
     */
    public static void startQuarkus() {
        Quarkus.run(args);
    }

    /**
     * Async exit quarkus
     */
    public static void asyncExitQuarkus() {
        Quarkus.asyncExit();
    }

    private static Command parseCommand(String[] args) {
        Command command = null;
        try {
            CommandLine commandLine = new CommandParser().parse(args);
            command = CommandFactory.createCommand(commandLine);
        } catch (ParseException e) {
            LOGGER.error("Failed to parse command line arguments:", e);
            new HelpCommand().execute();
        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid command: ", e);
            new HelpCommand().execute();
        }
        return command;
    }
}
