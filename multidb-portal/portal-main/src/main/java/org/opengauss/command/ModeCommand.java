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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.ModeCommandReceiver;

/**
 * mode command
 *
 * @since 2025/3/26
 */
public class ModeCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(ModeCommand.class);
    private final String[] args;

    ModeCommand(String[] args) {
        this.args = args;
    }

    @Override
    public void execute() {
        validateArgs(args);

        ModeCommandReceiver commandReceiver = new ModeCommandReceiver();
        String operation = args[0];
        switch (operation) {
            case "list":
                LOGGER.info("Start command to list migration modes");
                commandReceiver.list();
                break;
            case "add":
                LOGGER.info("Start command to add migration mode");
                validateOptionArgs(args, "add");
                commandReceiver.add(args[1]);
                break;
            case "update":
                LOGGER.info("Start command to update migration mode");
                validateOptionArgs(args, "update");
                commandReceiver.update(args[1]);
                break;
            case "delete":
                LOGGER.info("Start command to delete migration mode");
                validateOptionArgs(args, "delete");
                commandReceiver.delete(args[1]);
                break;
            case "template":
                LOGGER.info("Start command to get mode template file content");
                commandReceiver.template();
                break;
            default:
                throw new IllegalArgumentException("Unsupported migration mode operation: " + operation);
        }
    }

    private void validateArgs(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Missing argument for command: mode");
        }
    }

    private void validateOptionArgs(String[] args, String optionName) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument for command: mode " + optionName);
        }
    }
}
