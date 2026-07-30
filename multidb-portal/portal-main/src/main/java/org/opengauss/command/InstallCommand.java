/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.InstallCommandReceiver;

import java.util.List;

/**
 * install command
 *
 * @since 2025/3/26
 */
public class InstallCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(InstallCommand.class);

    private final List<String> args;
    private final boolean isForce;

    InstallCommand(List<String> components, boolean isForce) {
        this.args = components;
        this.isForce = isForce;
    }

    @Override
    public void execute() {
        InstallCommandReceiver commandReceiver = new InstallCommandReceiver();
        if (args.contains("dependencies")) {
            LOGGER.info("Start command to install dependencies");
            commandReceiver.dependencies(isForce);
            return;
        }
        if (args.contains("tools")) {
            LOGGER.info("Start command to install migration tools");
            commandReceiver.migrationTools();
            return;
        }
        if (args.contains("check")) {
            LOGGER.info("Start command to check installation");
            commandReceiver.check();
            return;
        }

        if (args.contains("chameleon")) {
            LOGGER.info("Start command to install chameleon");
            commandReceiver.chameleon();
        }
        if (args.contains("full_replicate")) {
            LOGGER.info("Start command to install full_replicate");
            commandReceiver.fullReplicate();
        }
        if (args.contains("milvus_migration_tool")) {
            LOGGER.info("Start command to install milvus_migration_tool");
            commandReceiver.milvusMigrationTool();
        }
        if (args.contains("elasticsearch_migration_tool")) {
            LOGGER.info("Start command to install elasticsearch_migration_tool");
            commandReceiver.elasticsearchMigrationTool();
        }
        if (args.contains("data_checker")) {
            LOGGER.info("Start command to install data-checker");
            commandReceiver.dataChecker();
        }
        if (args.contains("debezium")) {
            LOGGER.info("Start command to install debezium");
            commandReceiver.debezium();
        }
        if (args.contains("kafka")) {
            LOGGER.info("Start command to install kafka");
            commandReceiver.kafka();
        }
        LOGGER.info("Install command execute done");
    }
}
