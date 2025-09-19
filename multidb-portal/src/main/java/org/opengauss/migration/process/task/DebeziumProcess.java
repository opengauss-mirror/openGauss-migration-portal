/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.DebeziumConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.DebeziumHelper;
import org.opengauss.utils.ProcessUtils;

import java.io.IOException;
import java.util.Map;

/**
 * debezium process
 *
 * @since 2025/3/1
 */
public class DebeziumProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumProcess.class);

    private final ConfigFile connectorConfig;
    private final ConfigFile workerConfig;
    private final ConfigFile log4jConfig;
    private final Map<String, String> processEnv;

    public DebeziumProcess(String processName, TaskWorkspace taskWorkspace, ConfigFile connectorConfig,
                           ConfigFile workerConfig, ConfigFile log4jConfig, String commandPrefix,
                           Map<String, String> processEnv) {
        super(processName, taskWorkspace,
                DebeziumHelper.generateProcessStartCommand(connectorConfig, workerConfig, log4jConfig, commandPrefix),
                DebeziumHelper.generateProcessCheckCommand(connectorConfig, workerConfig));
        this.connectorConfig = connectorConfig;
        this.workerConfig = workerConfig;
        this.log4jConfig = log4jConfig;
        this.processEnv = processEnv;
    }

    @Override
    public void start() {
        if (!isStarted) {
            try {
                String workDirPath = taskWorkspace.getHomeDir();
                ProcessUtils.executeCommand(startCommand, workDirPath, DebeziumConstants.WAIT_PROCESS_START_MILLIS,
                        processEnv);
                LOGGER.info("{} started", processName);
                LOGGER.info("{} is running", processName);
            } catch (IOException | InterruptedException e) {
                throw new MigrationException("Failed to start Debezium process " + processName, e);
            }
            isStarted = true;
            isStopped = false;
            isNormal = true;
        }
    }

    @Override
    public boolean checkStatus() {
        if (!isStarted || isStopped) {
            return isNormal;
        }

        if (!isAlive() && !isStopped) {
            this.isNormal = false;
            this.isStopped = true;
            LOGGER.error("{} exit abnormally", processName);
        }
        return isNormal;
    }

    @Override
    public void waitExit() {
        throw new UnsupportedOperationException("Debezium process does not support waitExit");
    }
}
