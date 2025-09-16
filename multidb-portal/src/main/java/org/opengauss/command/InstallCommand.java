/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.InstallCommandReceiver;

/**
 * install command
 *
 * @since 2025/3/26
 */
public class InstallCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(InstallCommand.class);

    private final String component;
    private final boolean isForce;

    InstallCommand(String component, boolean isForce) {
        this.component = component;
        this.isForce = isForce;
    }

    @Override
    public void execute() {
        InstallCommandReceiver commandReceiver = new InstallCommandReceiver();
        switch (component) {
            case "dependencies":
                LOGGER.info("Start command to install dependencies");
                commandReceiver.dependencies(isForce);
                break;
            case "tools":
                LOGGER.info("Start command to install migration tools");
                commandReceiver.migrationTools();
                break;
            case "chameleon":
                LOGGER.info("Start command to install chameleon");
                commandReceiver.chameleon();
                break;
            case "og_datasync":
                LOGGER.info("Start command to install oG_datasync_full_migration");
                commandReceiver.ogDatasync();
                break;
            case "data_checker":
                LOGGER.info("Start command to install data-checker");
                commandReceiver.dataChecker();
                break;
            case "debezium":
                LOGGER.info("Start command to install debezium");
                commandReceiver.debezium();
                break;
            case "kafka":
                LOGGER.info("Start command to install kafka");
                commandReceiver.kafka();
                break;
            case "check":
                LOGGER.info("Start command to check installation");
                commandReceiver.check();
                break;
            default:
                throw new IllegalArgumentException("Unsupported component: " + component + " for install");
        }
    }
}
