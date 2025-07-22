/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.ChameleonConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.ChameleonHelper;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;

/**
 * chameleon process
 *
 * @since 2025/3/1
 */
public class ChameleonProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(ChameleonProcess.class);

    private final String chameleonOrder;

    public ChameleonProcess(String processName, TaskWorkspace taskWorkspace, String chameleonOrder) {
        super(processName, taskWorkspace, ChameleonHelper.generateProcessStartCommand(taskWorkspace, chameleonOrder),
                ChameleonHelper.generateProcessStartCommand(taskWorkspace, chameleonOrder));
        this.chameleonOrder = chameleonOrder;
    }

    @Override
    public void start() {
        if (isStarted) {
            return;
        }

        String workDirPath = Chameleon.getInstance().getChameleonHomeDirPath();
        String logPath = ChameleonHelper.generateFullMigrationLogPath(taskWorkspace);

        try {
            if (ChameleonConstants.ORDER_DETACH_REPLICA.equals(chameleonOrder)) {
                String[] interactArgs = new String[]{"YES"};
                ProcessUtils.executeInteractiveCommand(startCommand, workDirPath, logPath,
                        ChameleonConstants.WAIT_PROCESS_START_MILLIS, interactArgs);
            } else {
                ProcessUtils.executeCommand(startCommand, workDirPath, logPath,
                        ChameleonConstants.WAIT_PROCESS_START_MILLIS);
            }
            LOGGER.info("{} started", processName);
            LOGGER.info("{} is running", processName);
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Failed to start chameleon process " + processName, e);
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
                String logPath = ChameleonHelper.generateFullMigrationLogPath(taskWorkspace);
                String lastLine = FileUtils.readFileLastLine(logPath);
                String endFlag = chameleonOrder + " finished";

                isStopped = true;
                if (lastLine.contains(endFlag)) {
                    LOGGER.info("{} has finished", processName);
                } else {
                    isNormal = false;
                    LOGGER.error("{} exit abnormally", processName);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read chameleon process log, error :{}", e.getMessage());
        }

        return isNormal;
    }

    @Override
    public void waitExit() {
        while (isStarted && !isStopped) {
            ThreadUtils.sleep(1000);
            checkStatus();
        }
    }
}
