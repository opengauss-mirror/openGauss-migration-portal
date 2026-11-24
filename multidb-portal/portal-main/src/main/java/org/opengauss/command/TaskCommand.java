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
import org.opengauss.command.receiver.TaskCommandReceiver;

/**
 * Task command
 *
 * @since 2025/3/26
 */
public class TaskCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(TaskCommand.class);

    private final String[] args;

    TaskCommand(String[] args) {
        this.args = args;
    }

    @Override
    public void execute() {
        validateArgs(args);

        TaskCommandReceiver commandReceiver = new TaskCommandReceiver();
        String operation = args[0];
        switch (operation) {
            case "list":
                LOGGER.info("Start command to list migration tasks");
                commandReceiver.list();
                break;
            case "create":
                LOGGER.info("Start command to create migration task");
                validateCreateArgs(args);
                commandReceiver.create(args[1], args[2]);
                break;
            case "delete":
                LOGGER.info("Start command to delete migration task");
                validateDeleteArgs(args);
                commandReceiver.delete(args[1]);
                break;
            default:
                throw new IllegalArgumentException("Unsupported task operation: " + operation);
        }
    }

    private void validateArgs(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Missing argument for command: task");
        }
    }

    private void validateCreateArgs(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Missing argument for command: task create");
        }
    }

    private void validateDeleteArgs(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument for command: task delete");
        }
    }
}
