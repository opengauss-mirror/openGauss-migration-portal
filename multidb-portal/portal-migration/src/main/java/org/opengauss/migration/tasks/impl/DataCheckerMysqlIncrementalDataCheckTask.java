/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.domain.config.MysqlMigrationConfig;
import org.opengauss.migration.domain.dto.DataCheckerConfigBundle;
import org.opengauss.migration.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.tasks.phase.IncrementalDataCheckTask;
import org.opengauss.migration.tasks.tool.DataCheckerTask;

import java.util.List;

/**
 * Data-checker mysql incremental data check task
 *
 * @since 2025/5/12
 */
public class DataCheckerMysqlIncrementalDataCheckTask extends DataCheckerTask implements IncrementalDataCheckTask {
    private static final Logger LOGGER = LogManager.getLogger(DataCheckerMysqlIncrementalDataCheckTask.class);

    public DataCheckerMysqlIncrementalDataCheckTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            MysqlMigrationConfig migrationConfigDto, DataCheckerConfigBundle dataCheckerConfig) {
        super(processMonitor, migrationStopIndicator, taskWorkspace, migrationConfigDto, dataCheckerConfig);
    }

    @Override
    public void beforeTask() {
        cleanIncrementalCheckHistoryFiles();
        checkKafkaStatus();
    }

    @Override
    public void startTask() {
        configProcessPort();
        initIncrementalProcess();

        List<TaskProcess> processList = List.of(sourceProcess, sinkProcess, checkProcess);
        if (migrationStopIndicator.isStopped()) {
            return;
        }
        startCheckProcessList(processList);

        String signFilePath = DataCheckerHelper.getIncrementalProcessSignFilePath(taskWorkspace);
        if (checkStartSign(signFilePath)) {
            LOGGER.info("Start incremental data check process successfully");
        } else {
            LOGGER.warn("Has not checked incremental data check process start sign");
        }

        for (TaskProcess taskProcess : processList) {
            processMonitor.addProcess(taskProcess);
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
