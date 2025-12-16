/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.MilvusMigrationToolConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.MilvusMigrationToolHelper;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;
import java.util.Map;

/**
 * MilvusMigrationToolProcess
 *
 * @since 2025/10/23
 */
public class MilvusMigrationToolProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(MilvusMigrationToolProcess.class);

    private final String collectionName;
    private final String tableName;
    private final Map<String, String> processEnv;

    public MilvusMigrationToolProcess(String processName, TaskWorkspace taskWorkspace, String collectionName,
                                      String tableName, Map<String, String> processEnv) {
        super(processName, taskWorkspace,
                MilvusMigrationToolHelper.generateProcessStartCommand(collectionName, tableName),
                MilvusMigrationToolHelper.generateProcessStartCommand(collectionName, tableName));
        this.collectionName = collectionName;
        this.tableName = tableName;
        this.processEnv = processEnv;
    }

    @Override
    public void start() {
        if (isStarted) {
            return;
        }

        String workDirPath = taskWorkspace.getConfigFullDirPath();
        String logPath = MilvusMigrationToolHelper.generateFullMigrationLogPath(taskWorkspace, collectionName);

        try {
            ProcessUtils.executeCommand(startCommand, workDirPath, logPath,
                    MilvusMigrationToolConstants.WAIT_PROCESS_START_MILLIS, processEnv);
            LOGGER.info("Milvus collection '{}' to openGauss table '{}' migration process started",
                    collectionName, tableName);
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Failed to start Milvus migration process, collection: " + collectionName, e);
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
                String logPath = MilvusMigrationToolHelper.generateFullMigrationLogPath(taskWorkspace, collectionName);
                String lastLine = FileUtils.readFileLastLine(logPath);
                String endFlag = MilvusMigrationToolConstants.MIGRATION_END_FLAG;

                isStopped = true;
                if (lastLine.contains(endFlag)) {
                    LOGGER.info("Milvus collection '{}' to openGauss table '{}' migration process has finished",
                            collectionName, tableName);
                } else {
                    isNormal = false;
                    LOGGER.error("Milvus collection '{}' to openGauss table '{}' migration process exit abnormally",
                            collectionName, tableName);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read Milvus migration process log, collection: {}, error: {}",
                    collectionName, e.getMessage());
        }

        return isNormal;
    }

    @Override
    public void waitExit() {
        while (isStarted && !isStopped) {
            checkStatus();
            ThreadUtils.sleep(500);
        }
    }
}
