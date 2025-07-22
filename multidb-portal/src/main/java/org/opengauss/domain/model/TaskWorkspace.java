/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

import lombok.Getter;
import org.opengauss.constants.TaskConstants;
import org.opengauss.exceptions.TaskException;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.utils.FileUtils;

import java.io.IOException;

/**
 * workspace
 *
 * @since 2025/2/27
 */
@Getter
public class TaskWorkspace {
    private final String id;
    private final String homeDir;

    private final String configDirPath;
    private final String logsDirPath;
    private final String statusDirPath;
    private final String tmpDirPath;

    private final String configFullDirPath;
    private final String configFullDataCheckDirPath;
    private final String configIncrementalDirPath;
    private final String configIncrementalDataCheckDirPath;
    private final String configReverseDirPath;

    private final String logsFullDirPath;
    private final String logsFullDataCheckDirPath;
    private final String logsIncrementalDirPath;
    private final String logsIncrementalDataCheckDirPath;
    private final String logsReverseDirPath;

    private final String statusFullDirPath;
    private final String statusFullDataCheckDirPath;
    private final String statusIncrementalDirPath;
    private final String statusIncrementalDataCheckDirPath;
    private final String statusReverseDirPath;

    private final String quarkusPortFilePath;
    private final String sourceDbTypeFilePath;

    public TaskWorkspace(String taskId) {
        String portalWorkspaceDirPath = ApplicationConfig.getInstance().getPortalWorkspaceDirPath();
        id = taskId;
        homeDir = String.format("%s/%s%s", portalWorkspaceDirPath, TaskConstants.TASK_WORKSPACE_DIR_SUFFIX, taskId);

        configDirPath = String.format("%s/config", homeDir);
        logsDirPath = String.format("%s/logs", homeDir);
        statusDirPath = String.format("%s/status", homeDir);
        tmpDirPath = String.format("%s/tmp", homeDir);

        configFullDirPath = String.format("%s/full", configDirPath);
        configFullDataCheckDirPath = String.format("%s/data-check/full", configDirPath);
        configIncrementalDirPath = String.format("%s/incremental", configDirPath);
        configIncrementalDataCheckDirPath = String.format("%s/data-check/incremental", configDirPath);
        configReverseDirPath = String.format("%s/reverse", configDirPath);

        logsFullDirPath = String.format("%s/full", logsDirPath);
        logsFullDataCheckDirPath = String.format("%s/data-check/full", logsDirPath);
        logsIncrementalDirPath = String.format("%s/incremental", logsDirPath);
        logsIncrementalDataCheckDirPath = String.format("%s/data-check/incremental", logsDirPath);
        logsReverseDirPath = String.format("%s/reverse", logsDirPath);

        statusFullDirPath = String.format("%s/full", statusDirPath);
        statusFullDataCheckDirPath = String.format("%s/data-check/full", statusDirPath);
        statusIncrementalDirPath = String.format("%s/incremental", statusDirPath);
        statusIncrementalDataCheckDirPath = String.format("%s/data-check/incremental", statusDirPath);
        statusReverseDirPath = String.format("%s/reverse", statusDirPath);

        sourceDbTypeFilePath = String.format("%s/%s", configDirPath, TaskConstants.SOURCE_DB_TYPE_CONFIG_FILE_NAME);
        quarkusPortFilePath = String.format("%s/%s", configDirPath, TaskConstants.QUARKUS_PORT_FILE_NAME);
    }

    /**
     * create task workspace directory structure
     */
    public void create() {
        try {
            FileUtils.createDirectories(homeDir, configDirPath, logsDirPath, statusDirPath, tmpDirPath,
                    configFullDirPath, configFullDataCheckDirPath, configIncrementalDirPath,
                    configIncrementalDataCheckDirPath, configReverseDirPath,
                    logsFullDirPath, logsFullDataCheckDirPath, logsIncrementalDirPath,
                    logsIncrementalDataCheckDirPath, logsReverseDirPath,
                    statusFullDirPath, statusFullDataCheckDirPath, statusIncrementalDirPath,
                    statusIncrementalDataCheckDirPath, statusReverseDirPath);

            FileUtils.createFile(sourceDbTypeFilePath);
            FileUtils.createFile(quarkusPortFilePath);
        } catch (IOException e) {
            throw new TaskException("Failed to create workspace directories", e);
        }
    }

    /**
     * delete task workspace directory
     */
    public void delete() {
        try {
            FileUtils.deletePath(homeDir);
        } catch (IOException e) {
            throw new TaskException("Failed to clean up task workspace directory: " + homeDir, e);
        }
    }
}
