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

package org.opengauss.command;

import org.apache.commons.cli.CommandLine;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * Command factory
 *
 * @since 2025/3/26
 */
public class CommandFactory {
    private static final Map<String, Function<CommandLine, Command>> COMMAND_MAP = Map.of(
            "help", CommandFactory::generateHelpCommand,
            "install", CommandFactory::generateInstallCommand,
            "uninstall", CommandFactory::generateUninstallCommand,
            "kafka", CommandFactory::generateKafkaCommand,
            "mode", CommandFactory::generateModeCommand,
            "task", CommandFactory::generateTaskCommand,
            "migration", CommandFactory::generateMigrationCommand
    );

    /**
     * Create command
     *
     * @param cmd command line
     * @return command
     */
    public static Command createCommand(CommandLine cmd) {
        for (Map.Entry<String, Function<CommandLine, Command>> entry : COMMAND_MAP.entrySet()) {
            if (cmd.hasOption(entry.getKey())) {
                return entry.getValue().apply(cmd);
            }
        }

        throw new IllegalArgumentException("Invalid command");
    }

    private static HelpCommand generateHelpCommand(CommandLine cmd) {
        return new HelpCommand();
    }

    private static InstallCommand generateInstallCommand(CommandLine cmd) {
        String[] installArgs = cmd.getOptionValues("install");
        if (installArgs == null || installArgs.length == 0) {
            throw new IllegalArgumentException("Command install requires at least one argument");
        }

        if (cmd.hasOption("force")) {
            return new InstallCommand(Arrays.stream(installArgs).toList(), true);
        } else {
            return new InstallCommand(Arrays.stream(installArgs).toList(), false);
        }
    }

    private static UninstallCommand generateUninstallCommand(CommandLine cmd) {
        return new UninstallCommand(cmd.getOptionValue("uninstall"));
    }

    private static KafkaCommand generateKafkaCommand(CommandLine cmd) {
        return new KafkaCommand(cmd.getOptionValue("kafka"));
    }

    private static ModeCommand generateModeCommand(CommandLine cmd) {
        return new ModeCommand(cmd.getOptionValues("mode"));
    }

    private static TaskCommand generateTaskCommand(CommandLine cmd) {
        return new TaskCommand(cmd.getOptionValues("task"));
    }

    private static MigrationCommand generateMigrationCommand(CommandLine cmd) {
        String[] args = cmd.getOptionValues("migration");
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("Command migration requires two arguments: operation taskId");
        }
        boolean hasDetail = cmd.hasOption("detail");
        if (hasDetail) {
            return new MigrationCommand(args[0], args[1], true);
        }
        return new MigrationCommand(args[0], args[1]);
    }
}