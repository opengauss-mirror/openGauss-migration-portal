/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.DataCheckerConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.enums.DataCheckerProcessType;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;

/**
 * data checker process
 *
 * @since 2025/3/1
 */
public class DataCheckerProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(DataCheckerProcess.class);

    private final DataCheckerProcessType processType;
    private final ConfigFile processConfig;
    private final boolean isFullMigration;

    public DataCheckerProcess(String processName, TaskWorkspace taskWorkspace, ConfigFile processConfig,
                              DataCheckerProcessType processType, String jvmPrefixOptions, boolean isFullMigration) {
        super(processName, taskWorkspace,
                DataCheckerHelper.generateProcessStartCommand(processType, processConfig.getFilePath(),
                        jvmPrefixOptions),
                DataCheckerHelper.generateProcessCheckCommand(processType, processConfig.getFilePath()));

        this.processType = processType;
        this.processConfig = processConfig;
        this.isFullMigration = isFullMigration;
    }

    @Override
    public void start() {
        if (!isStarted) {
            String workDirPath = taskWorkspace.getHomeDir();
            try {
                ProcessUtils.executeCommand(startCommand, workDirPath, DataCheckerConstants.WAIT_PROCESS_START_MILLIS);
                LOGGER.info("{} started", processName);
                LOGGER.info("{} is running", processName);
            } catch (IOException | InterruptedException e) {
                throw new MigrationException("Failed to start DataChecker process: " + processName, e);
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
            if (isFullMigration && checkExitSign()) {
                LOGGER.info("{} has finished", processName);
            } else {
                isNormal = false;
                LOGGER.error("{} exit abnormally", processName);
            }
            isStopped = true;
        }
        return isNormal;
    }

    @Override
    public void waitExit() {
        if (!isFullMigration) {
            return;
        }

        while (isStarted && !isStopped) {
            ThreadUtils.sleep(1000);
            checkStatus();
        }
    }

    private boolean checkExitSign() {
        String signFilePath = isFullMigration ? DataCheckerHelper.getFullProcessSignFilePath(taskWorkspace)
                : DataCheckerHelper.getIncrementalProcessSignFilePath(taskWorkspace);
        try {
            String fileContents = FileUtils.readFileContents(signFilePath);
            String stopSign = DataCheckerHelper.getProcessStopSign(processType);
            if (fileContents.contains(stopSign)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("Failed to check data check process exit sign, error: {}", e.getMessage());
            return false;
        }
        return false;
    }
}
