/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.exceptions.KafkaException;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;

/**
 * Confluent process
 *
 * @since 2025/4/18
 */
@Getter
public class ConfluentProcess implements Process {
    private static final Logger LOGGER = LogManager.getLogger(ConfluentProcess.class);

    private final String logPath;
    private final long startWaitTime;
    private final String processName;
    private final String startCommand;
    private final String checkCommand;

    private int pid;

    public ConfluentProcess(String processName, String startCommand, String checkCommand,
                            String logPath, long startWaitTime) {
        this.processName = processName;
        this.startCommand = startCommand;
        this.checkCommand = checkCommand;
        this.startWaitTime = startWaitTime;
        this.logPath = logPath;
    }

    @Override
    public void start() {
        try {
            if (!isAlive()) {
                String workDirPath = ApplicationConfig.getInstance().getPortalTmpDirPath();
                ProcessUtils.executeCommand(startCommand, workDirPath, logPath, startWaitTime);
            } else {
                LOGGER.info("Process {} is already started.", processName);
            }
        } catch (IOException | InterruptedException e) {
            throw new KafkaException("Failed to start process " + processName, e);
        }
    }

    @Override
    public void stop() {
        if (isAlive()) {
            try {
                ProcessUtils.killProcessByCommandSnippet(checkCommand, false);
            } catch (IOException | InterruptedException e) {
                LOGGER.warn("Kill {} with error: {}", processName, e.getMessage());
            }

            waitProcessExit();
        }
    }

    @Override
    public boolean checkStatus() {
        if (isAlive()) {
            return true;
        } else {
            LOGGER.error("Process {} exit abnormally.", processName);
            return false;
        }
    }

    @Override
    public boolean isAlive() {
        try {
            int commandPid = ProcessUtils.getCommandPid(checkCommand);
            if (commandPid == -1) {
                pid = ProcessUtils.getCommandPid(checkCommand);
            } else {
                pid = commandPid;
            }

            return pid != -1;
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Check {} status with error: {}", processName, e.getMessage());
            return false;
        }
    }

    private void waitProcessExit() {
        int oneSecond = 1000;
        int processStopTime = 5000;
        while (processStopTime > 0) {
            ThreadUtils.sleep(oneSecond);
            processStopTime -= oneSecond;

            if (!isAlive()) {
                LOGGER.info("{} stopped", processName);
                return;
            }
        }

        try {
            ProcessUtils.killProcessByCommandSnippet(checkCommand, true);
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Kill {} with error: {}", processName, e.getMessage());
        }

        if (isAlive()) {
            LOGGER.error("Failed to stop {}, please kill it manually, pid: {}", processName, pid);
        } else {
            LOGGER.info("{} stopped", processName);
        }
    }
}
