/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.ElasticsearchMigrationToolConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.ElasticsearchMigrationToolHelper;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;
import java.util.Map;

/**
 * ElasticsearchMigrationToolProcess
 *
 * @since 2025/10/23
 */
public class ElasticsearchMigrationToolProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchMigrationToolProcess.class);

    private final String indexName;
    private final String tableName;
    private final Map<String, String> processEnv;

    public ElasticsearchMigrationToolProcess(String processName, TaskWorkspace taskWorkspace, String indexName,
                                             String tableName, Map<String, String> processEnv) {
        super(processName, taskWorkspace,
                ElasticsearchMigrationToolHelper.generateProcessStartCommand(indexName, tableName),
                ElasticsearchMigrationToolHelper.generateProcessStartCommand(indexName, tableName));
        this.indexName = indexName;
        this.tableName = tableName;
        this.processEnv = processEnv;
    }

    @Override
    public void start() {
        if (isStarted) {
            return;
        }

        String workDirPath = taskWorkspace.getConfigFullDirPath();
        String logPath = ElasticsearchMigrationToolHelper.getFullMigrationLogPath(taskWorkspace, indexName);

        try {
            ProcessUtils.executeCommand(startCommand, workDirPath, logPath,
                    ElasticsearchMigrationToolConstants.WAIT_PROCESS_START_MILLIS, processEnv);
            LOGGER.info("Elasticsearch index '{}' table '{}' migration process started", indexName, tableName);
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Failed to start Elasticsearch migration process, index: " + indexName, e);
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
                String logPath = ElasticsearchMigrationToolHelper.getFullMigrationLogPath(taskWorkspace, indexName);
                String lastLine = FileUtils.readFileLastLine(logPath);
                String endFlag = ElasticsearchMigrationToolConstants.MIGRATION_END_FLAG;

                isStopped = true;
                if (lastLine.contains(endFlag)) {
                    LOGGER.info("Elasticsearch index '{}' table '{}' migration process has finished",
                            indexName, tableName);
                } else {
                    isNormal = false;
                    LOGGER.error("Elasticsearch index '{}' table '{}' migration process exit abnormally",
                            indexName, tableName);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read Elasticsearch migration process log, index: {}, error: {}",
                    indexName, e.getMessage());
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
