/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.tool.ChameleonConstants;
import org.opengauss.domain.model.ChameleonConfigBundle;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.exceptions.TaskException;
import org.opengauss.migration.helper.config.ChameleonMysqlMigrationConfigHelper;
import org.opengauss.migration.helper.tool.ChameleonHelper;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.migration.process.task.ChameleonProcess;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;

import java.io.IOException;
import java.util.List;

/**
 * chameleon task
 *
 * @since 2025/3/20
 */
public abstract class ChameleonTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(ChameleonTask.class);

    private final MigrationStopIndicator migrationStopIndicator;
    private final ConfigFile chameleonConfig;

    private TaskProcess currentProcess;

    protected ChameleonTask(TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
                            ChameleonConfigBundle chameleonConfig) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.chameleonConfig = chameleonConfig.getConfigFile();
    }

    /**
     * Prepare migration
     */
    protected void prepareMigration() {
        cleanHistoryFiles();
        prepareConfigFile();

        TaskProcess dropSchemaProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_DROP_REPLICA_SCHEMA,
                taskWorkspace, ChameleonConstants.ORDER_DROP_REPLICA_SCHEMA);
        TaskProcess createSchemaProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_CREATE_REPLICA_SCHEMA,
                taskWorkspace, ChameleonConstants.ORDER_CREATE_REPLICA_SCHEMA);
        TaskProcess addSourceProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_ADD_SOURCE, taskWorkspace,
                ChameleonConstants.ORDER_ADD_SOURCE);

        startProcessAndWaitExit(dropSchemaProcess);
        startProcessAndWaitExit(createSchemaProcess);
        startProcessAndWaitExit(addSourceProcess);
    }

    /**
     * Start migration table
     */
    protected void tableMigration() {
        TaskProcess initRelicaProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_INIT_REPLICA, taskWorkspace,
                ChameleonConstants.ORDER_INIT_REPLICA);
        currentProcess = initRelicaProcess;
        initRelicaProcess.start();
        LOGGER.info("Full migration table is running...");
    }

    /**
     * Wait table migration exit
     */
    protected void waitTableMigrationExit() {
        if (currentProcess != null
                && currentProcess.getProcessName().equals(ProcessNameConstants.CHAMELEON_INIT_REPLICA)) {
            currentProcess.waitExit();
            if (!currentProcess.checkStatus()) {
                throw new MigrationException("Failed to migrate table, please check the log for details.");
            }
        }
    }

    /**
     * Start and wait exit migration trigger
     */
    protected void triggerMigration() {
        TaskProcess triggerProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_START_TRIGGER_REPLICA,
                taskWorkspace, ChameleonConstants.ORDER_START_TRIGGER_REPLICA);
        startProcessAndWaitExit(triggerProcess);
    }

    /**
     * Start and wait exit migration view
     */
    protected void viewMigration() {
        TaskProcess viewProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_START_VIEW_REPLICA, taskWorkspace,
                ChameleonConstants.ORDER_START_VIEW_REPLICA);
        startProcessAndWaitExit(viewProcess);
    }

    /**
     * Start and wait exit migration function
     */
    protected void functionMigration() {
        TaskProcess funcProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_START_FUNC_REPLICA, taskWorkspace,
                ChameleonConstants.ORDER_START_FUNC_REPLICA);
        startProcessAndWaitExit(funcProcess);
    }

    /**
     * Start and wait exit migration procedure
     */
    protected void procedureMigration() {
        TaskProcess procProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_START_PROC_REPLICA, taskWorkspace,
                ChameleonConstants.ORDER_START_PROC_REPLICA);
        startProcessAndWaitExit(procProcess);
    }

    /**
     * Start and wait exit migration foreign key
     */
    protected void foreignKeyMigration() {
        TaskProcess detachReplicaProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_DETACH_REPLICA,
                taskWorkspace, ChameleonConstants.ORDER_DETACH_REPLICA);
        detachReplicaProcess.start();
        detachReplicaProcess.waitExit();
        if (!detachReplicaProcess.checkStatus()) {
            throw new MigrationException("Failed to detach replica, please check the log for details.");
        }
    }

    /**
     * After migration
     */
    protected void afterMigration() {
        TaskProcess dropSchemaProcess = new ChameleonProcess(ProcessNameConstants.CHAMELEON_DROP_REPLICA_SCHEMA,
                taskWorkspace, ChameleonConstants.ORDER_DROP_REPLICA_SCHEMA);
        dropSchemaProcess.start();
        dropSchemaProcess.waitExit();

        cleanJsonFiles();
    }

    /**
     * Stop current process
     */
    protected void stop() {
        if (currentProcess != null) {
            currentProcess.stop();
        }
    }

    private void cleanHistoryFiles() {
        String pidDirPath = ChameleonMysqlMigrationConfigHelper.generatePidDir(taskWorkspace);
        String csvDirPath = ChameleonMysqlMigrationConfigHelper.generateCsvDir(taskWorkspace) + "/chameleon";
        String logPath = ChameleonHelper.generateFullMigrationLogPath(taskWorkspace);
        String statusDirPath = taskWorkspace.getStatusFullDirPath();
        try {
            FileUtils.deletePath(pidDirPath);
            FileUtils.deletePath(csvDirPath);
            FileUtils.deletePath(logPath);
            FileUtils.cleanDirectory(statusDirPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to delete chameleon history files, error message: {}", e.getMessage());
        }

        cleanJsonFiles();
    }

    private void cleanJsonFiles() {
        List<String> jsonFilePaths = ChameleonHelper.getAllStatusFilePathList(taskWorkspace);
        try {
            for (String jsonFilePath : jsonFilePaths) {
                FileUtils.deletePath(jsonFilePath);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to delete chameleon json files, error message: {}", e.getMessage());
        }
    }

    private void startProcessAndWaitExit(TaskProcess process) {
        if (!migrationStopIndicator.isStopped()) {
            currentProcess = process;
            process.start();
            process.waitExit();
            if (!process.checkStatus()) {
                throw new MigrationException(
                        process.getProcessName() + " exit abnormally, please check the log for details.");
            }
        }
    }

    private void prepareConfigFile() {
        Chameleon chameleon = Chameleon.getInstance();
        String chameleonPath = chameleon.getChameleonPath();
        String prepareConfigDirCommand = String.format("%s %s", chameleonPath,
                ChameleonConstants.ORDER_SET_CONFIGURATION_FILES);
        String logPath = ChameleonHelper.generateFullMigrationLogPath(taskWorkspace);
        try {
            String workDirPath = chameleon.getChameleonHomeDirPath();
            ProcessUtils.executeCommand(prepareConfigDirCommand, workDirPath, logPath,
                    ChameleonConstants.WAIT_PROCESS_START_MILLIS);
        } catch (IOException | InterruptedException e) {
            throw new TaskException("Failed to set configuration files", e);
        }

        String targetFileName = ChameleonHelper.generateFullMigrationConfigFileName(taskWorkspace);
        String targetFilePath = String.format("%s/%s", ChameleonConstants.PG_CHAMELEON_CONFIG_DIR_PATH, targetFileName);
        targetFilePath = targetFilePath.replaceFirst("~", System.getProperty("user.home"));
        try {
            FileUtils.copyFile(chameleonConfig.getFilePath(), targetFilePath);
        } catch (IOException e) {
            throw new TaskException("Failed to copy full migration config file", e);
        }
    }
}
