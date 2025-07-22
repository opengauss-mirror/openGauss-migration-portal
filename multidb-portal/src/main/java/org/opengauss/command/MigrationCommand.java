/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.MigrationCommandReceiver;
import org.opengauss.utils.StringUtils;

/**
 * migration command
 *
 * @since 2025/3/26
 */
public class MigrationCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(MigrationCommand.class);

    private final String operation;
    private final String taskId;
    private final boolean isDetail;

    MigrationCommand(String operation, String taskId) {
        this(operation, taskId, false);
    }

    MigrationCommand(String operation, String taskId, boolean isDetail) {
        this.operation = operation;
        this.taskId = taskId;
        this.isDetail = isDetail;
    }

    @Override
    public void execute() {
        validateArgs();

        MigrationCommandReceiver migrationExecutor = new MigrationCommandReceiver(taskId);
        switch (operation) {
            case "start":
                LOGGER.info("Start command to start migration");
                migrationExecutor.start();
                break;
            case "status":
                LOGGER.info("Start command to check migration status");
                migrationExecutor.status(isDetail);
                break;
            case "stop":
                LOGGER.info("Start command to stop migration");
                migrationExecutor.stop();
                break;
            case "stop_incremental":
                LOGGER.info("Start command to stop incremental migration");
                migrationExecutor.stopIncremental();
                break;
            case "resume_incremental":
                LOGGER.info("Start command to resume incremental migration");
                migrationExecutor.resumeIncremental();
                break;
            case "restart_incremental":
                LOGGER.info("Start command to restart incremental migration");
                migrationExecutor.restartIncremental();
                break;
            case "start_reverse":
                LOGGER.info("Start command to start reverse migration");
                migrationExecutor.startReverse();
                break;
            case "resume_reverse":
                LOGGER.info("Start command to resume reverse migration");
                migrationExecutor.resumeReverse();
                break;
            case "restart_reverse":
                LOGGER.info("Start command to restart reverse migration");
                migrationExecutor.restartReverse();
                break;
            case "stop_reverse":
                LOGGER.info("Start command to stop reverse migration");
                migrationExecutor.stopReverse();
                break;
            default:
                throw new IllegalArgumentException("Unsupported migration operation: " + operation);
        }
    }

    private void validateArgs() {
        if (StringUtils.isNullOrBlank(operation) || StringUtils.isNullOrBlank(taskId)) {
            throw new IllegalArgumentException("Migration operation and workspace id cannot be empty");
        }
    }
}
