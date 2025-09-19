/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.OgDatasyncConfig;
import org.opengauss.constants.tool.OgDatasyncConstants;
import org.opengauss.domain.dto.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.MigrationContext;
import org.opengauss.migration.config.AbstractMigrationJobConfig;
import org.opengauss.migration.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.helper.tool.OgDatasyncHelper;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * oG_datasync_full_migration process
 *
 * @since 2025/5/29
 */
public class OgDatasyncProcess extends TaskProcess {
    private static final Logger LOGGER = LogManager.getLogger(OgDatasyncProcess.class);

    private final ConfigFile fullConfig;
    private final String sourceDbType;
    private final String order;

    public OgDatasyncProcess(String processName, TaskWorkspace taskWorkspace, ConfigFile fullConfig,
                             String sourceDbType, String order, String jvmPrefixOptions) {
        super(processName, taskWorkspace,
                OgDatasyncHelper.generateProcessStartCommand(fullConfig, sourceDbType, order, jvmPrefixOptions),
                OgDatasyncHelper.generateProcessCheckCommand(fullConfig, sourceDbType, order, jvmPrefixOptions));

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
        String logPath = OgDatasyncHelper.generateFullMigrationLogPath(taskWorkspace);

        try {
            ProcessUtils.executeCommand(startCommand, workDirPath, logPath,
                    OgDatasyncConstants.WAIT_PROCESS_START_MILLIS, generateProcessEnv());
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
                String logPath = OgDatasyncHelper.generateFullMigrationLogPath(taskWorkspace);
                String endFlag = OgDatasyncHelper.getProcessStopSign(order);
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
            LOGGER.warn("Failed to read oG_datasync_full_migration process log, error :{}", e.getMessage());
        }

        return isNormal;
    }

    private Map<String, String> generateProcessEnv() {
        AbstractMigrationJobConfig migrationJobConfig = MigrationContext.getInstance().getMigrationJobConfig();
        if (!(migrationJobConfig instanceof PgsqlMigrationJobConfig pgsqlMigrationJobConfig)) {
            return new HashMap<>();
        }
        PgsqlMigrationConfigDto migrationConfigDto = pgsqlMigrationJobConfig.getMigrationConfigDto();

        Map<String, String> env = new HashMap<>();
        if (migrationConfigDto.isUseInteractivePassword()) {
            env.put(OgDatasyncConfig.ENABLE_ENV_PASSWORD, "true");
            env.put(OgDatasyncConfig.OG_CONN_PASSWORD, migrationConfigDto.getOpengaussDatabasePassword());
            env.put(OgDatasyncConfig.SOURCE_DB_CONN_PASSWORD, migrationConfigDto.getPgsqlDatabasePassword());
        }
        return env;
    }
}
