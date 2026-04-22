/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.UninstallCommandReceiver;

/**
 * uninstall command
 *
 * @since 2025/3/28
 */
public class UninstallCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(UninstallCommand.class);
    private final String component;

    UninstallCommand(String component) {
        this.component = component;
    }

    @Override
    public void execute() {
        if (component.equals("tools")) {
            UninstallCommandReceiver commandReceiver = new UninstallCommandReceiver();
            LOGGER.info("Start command to uninstall migration tools");
            commandReceiver.migrationTools();
        } else {
            throw new IllegalArgumentException("Unsupported component: " + component + "for uninstall");
        }
    }
}
