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
