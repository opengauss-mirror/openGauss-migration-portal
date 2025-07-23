/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.migration.tools.DataChecker;
import org.opengauss.migration.tools.Debezium;
import org.opengauss.migration.tools.FullMigrationTool;
import org.opengauss.migration.tools.Kafka;

/**
 * uninstall command receiver
 *
 * @since 2025/3/29
 */
public class UninstallCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(UninstallCommandReceiver.class);

    /**
     * uninstall all migration tools
     */
    public void migrationTools() {
        Kafka.getInstance().unInstall();
        Chameleon.getInstance().unInstall();
        FullMigrationTool.getInstance().unInstall();
        DataChecker.getInstance().unInstall();
        Debezium.getInstance().unInstall();
        LOGGER.info("Uninstall all migration tools successfully");
    }
}
