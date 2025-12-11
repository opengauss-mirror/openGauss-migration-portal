/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.domain.tool.config.DataCheckerConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.tasks.phase.FullDataCheckTask;
import org.opengauss.migration.tasks.tool.DataCheckerTask;

import java.util.List;

/**
 * Data-checker mysql full data check task
 *
 * @since 2025/5/12
 */
public class DataCheckerMysqlFullDataCheckTask extends DataCheckerTask implements FullDataCheckTask {
    private static final Logger LOGGER = LogManager.getLogger(DataCheckerMysqlFullDataCheckTask.class);

    public DataCheckerMysqlFullDataCheckTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            MysqlMigrationConfigDto migrationConfigDto, DataCheckerConfigBundle dataCheckerConfig) {
        super(processMonitor, migrationStopIndicator, taskWorkspace, migrationConfigDto, dataCheckerConfig);
    }

    @Override
    public void beforeTask() {
        cleanFullCheckHistoryFiles();
        checkKafkaStatus();
    }

    @Override
    public void startTask() {
        configProcessPort();
        initFullProcess();

        List<TaskProcess> processList = List.of(sourceProcess, sinkProcess, checkProcess);
        if (migrationStopIndicator.isStopped()) {
            return;
        }
        startCheckProcessList(processList);

        String signFilePath = DataCheckerHelper.getFullProcessSignFilePath(taskWorkspace);
        if (checkStartSign(signFilePath)) {
            LOGGER.info("Start full data check process successfully");
        } else {
            LOGGER.warn("Has not checked full data check process start sign");
        }

        for (TaskProcess taskProcess : processList) {
            taskProcess.waitExit();
            if (!taskProcess.checkStatus()) {
                throw new MigrationException("Full data check failed, please check the log for details.");
            }
        }
    }

    @Override
    public void stopTask() {
        super.stop();
    }

    @Override
    public void afterTask() {
    }
}
