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

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.migration.tools.DataChecker;
import org.opengauss.migration.tools.Debezium;
import org.opengauss.migration.tools.FullReplicateTool;
import org.opengauss.migration.tools.ElasticsearchMigrationTool;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.migration.tools.MilvusMigrationTool;

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
        FullReplicateTool.getInstance().unInstall();
        MilvusMigrationTool.getInstance().unInstall();
        ElasticsearchMigrationTool.getInstance().unInstall();
        DataChecker.getInstance().unInstall();
        Debezium.getInstance().unInstall();
        LOGGER.info("Uninstall all migration tools successfully");
    }
}
