/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.FullMigrationToolConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.FullMigrationToolHelper;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;

/**
 * full migration tool process
 *
 * @since 2025/5/29
 */
public class FullMigrationToolProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(FullMigrationToolProcess.class);

    private final ConfigFile fullConfig;
    private final String sourceDbType;
    private final String order;

    public FullMigrationToolProcess(String processName, TaskWorkspace taskWorkspace, ConfigFile fullConfig,
                                    String sourceDbType, String order, String jvmPrefixOptions) {
        super(processName, taskWorkspace,
                FullMigrationToolHelper.generateProcessStartCommand(fullConfig, sourceDbType, order, jvmPrefixOptions),
                FullMigrationToolHelper.generateProcessCheckCommand(fullConfig, sourceDbType, order, jvmPrefixOptions));

        this.fullConfig = fullConfig;
        this.sourceDbType = sourceDbType;
        this.order = order;
    }

    @Override
    public void waitExit() {
        while (isStarted && !isStopped) {
            ThreadUtils.sleep(1000);
            checkStatus();
        }
    }

    @Override
    public void start() {
        if (isStarted) {
            return;
        }

        String workDirPath = taskWorkspace.getStatusFullDirPath();
        String logPath = FullMigrationToolHelper.generateFullMigrationLogPath(taskWorkspace);

        try {
            ProcessUtils.executeCommand(startCommand, workDirPath, logPath,
                    FullMigrationToolConstants.WAIT_PROCESS_START_MILLIS);
            LOGGER.info("{} started", processName);
            LOGGER.info("{} is running", processName);
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Failed to start full migration process: " + processName, e);
        }

        isStarted = true;
        isStopped = false;
        isNormal = true;
    }

    @Override
    public boolean checkStatus() {
        if (!isStarted || isStopped) {
            return isNormal;
        }

        try {
            if (!isAlive() && !isStopped) {
                String logPath = FullMigrationToolHelper.generateFullMigrationLogPath(taskWorkspace);
                String endFlag = FullMigrationToolHelper.getProcessStopSign(order);
                String lastLine = FileUtils.readFileLastLine(logPath);

                if (lastLine.contains(endFlag)) {
                    LOGGER.info("{} has finished", processName);
                } else {
                    isNormal = false;
                    LOGGER.error("{} exit abnormally", processName);
                }
                isStopped = true;
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read full migration tool process log, error :{}", e.getMessage());
        }

        return isNormal;
    }
}
