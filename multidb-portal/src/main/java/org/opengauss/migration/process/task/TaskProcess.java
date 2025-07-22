/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.process.Process;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;

/**
 * task process
 *
 * @since 2025/3/1
 */
@Getter
public abstract class TaskProcess implements Process {
    private static final Logger LOGGER = LogManager.getLogger(TaskProcess.class);

    /**
     * Process name
     */
    protected final String processName;

    /**
     * Task workspace
     */
    protected final TaskWorkspace taskWorkspace;

    /**
     * Start command
     */
    protected final String startCommand;

    /**
     * Check command
     */
    protected final String checkCommand;

    /**
     * Is process started
     */
    protected volatile boolean isStarted = false;

    /**
     * Is process stopped
     */
    protected volatile boolean isStopped = false;

    /**
     * Is process normally
     */
    protected boolean isNormal = true;

    private int pid;

    protected TaskProcess(String processName, TaskWorkspace taskWorkspace, String startCommand, String checkCommand) {
        this.taskWorkspace = taskWorkspace;
        this.processName = processName;
        this.startCommand = startCommand;
        this.checkCommand = checkCommand;
    }

    /**
     * Wait process exit
     */
    public abstract void waitExit();

    @Override
    public void stop() {
        if (!isStopped || isAlive()) {
            isStopped = true;
            try {
                ProcessUtils.killProcessByCommandSnippet(checkCommand, false);
            } catch (IOException | InterruptedException e) {
                LOGGER.warn("Kill {} with error: {}", processName, e.getMessage());
            }

            waitProcessExit();
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
