/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.DataCheckerCheckConfig;
import org.opengauss.constants.config.DataCheckerSinkConfig;
import org.opengauss.constants.config.DataCheckerSourceConfig;
import org.opengauss.constants.tool.DataCheckerConstants;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.tool.config.DataCheckerConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DataCheckerProcessType;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DataCheckerProcess;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.PortUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * data checker task
 *
 * @since 2025/3/20
 */
public abstract class DataCheckerTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(DataCheckerTask.class);

    /**
     * Migration stop indicator
     */
    protected final MigrationStopIndicator migrationStopIndicator;

    /**
     * Process monitor
     */
    protected final ProcessMonitor processMonitor;

    /**
     * Migration config dto
     */
    protected final MysqlMigrationConfigDto migrationConfigDto;

    /**
     * Data checker source config
     */
    protected final ConfigFile sourceConfig;

    /**
     * Data checker sink config
     */
    protected final ConfigFile sinkConfig;

    /**
     * Data checker check config
     */
    protected final ConfigFile checkConfig;

    /**
     * Data checker source process
     */
    protected TaskProcess sourceProcess;

    /**
     * Data checker sink process
     */
    protected TaskProcess sinkProcess;

    /**
     * Data checker check process
     */
    protected TaskProcess checkProcess;

    protected DataCheckerTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            MysqlMigrationConfigDto migrationConfigDto, DataCheckerConfigBundle dataCheckerConfig) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.processMonitor = processMonitor;
        this.migrationConfigDto = migrationConfigDto;
        this.checkConfig = dataCheckerConfig.getCheckConfigFile();
        this.sinkConfig = dataCheckerConfig.getSinkConfigFile();
        this.sourceConfig = dataCheckerConfig.getSourceConfigFile();
    }

    /**
     * Clean full data check history files
     */
    protected void cleanFullCheckHistoryFiles() {
        String dataPath = DataCheckerHelper.generateFullDataCheckDataPath(taskWorkspace);
        String logPath = DataCheckerHelper.generateFullDataCheckLogsDirPath(taskWorkspace);
        try {
            FileUtils.cleanDirectory(dataPath);
            FileUtils.cleanDirectory(logPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to clean full data check history files", e);
        }
    }

    /**
     * Clean incremental data check history files
     */
    protected void cleanIncrementalCheckHistoryFiles() {
        String dataPath = DataCheckerHelper.generateIncrementalDataCheckDataPath(taskWorkspace);
        String logPath = DataCheckerHelper.generateIncrementalDataCheckLogsDirPath(taskWorkspace);
        try {
            FileUtils.cleanDirectory(dataPath);
            FileUtils.cleanDirectory(logPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to clean full data check history files", e);
        }
    }

    /**
     * Check kafka status
     */
    protected void checkKafkaStatus() {
        Kafka kafka = Kafka.getInstance();
        if (!kafka.status()) {
            LOGGER.warn("Before starting data check task, check for Kafka server is abnormal, restarting Kafka...");
            if (!Kafka.getInstance().restart()) {
                throw new MigrationException("Failed to restart Kafka before start data check task");
            }
        }
    }

    /**
     * Start check process list
     *
     * @param processList process list
     */
    protected void startCheckProcessList(List<TaskProcess> processList) {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(processList.size());
            processList.parallelStream().forEach(taskProcess -> {
                taskProcess.start();
                countDownLatch.countDown();
            });

            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new MigrationException("Interrupted while waiting for data check process to start", e);
        }
    }

    /**
     * Stop check processes
     */
    protected void stop() {
        if (sourceProcess != null) {
            sourceProcess.stop();
        }
        if (sinkProcess != null) {
            sinkProcess.stop();
        }
        if (checkProcess != null) {
            checkProcess.stop();
        }
    }

    /**
     * Init full check processes
     */
    protected void initFullProcess() {
        String sourcePrefixOptions = migrationConfigDto.getFullCheckSourceProcessJvm();
        String sinkPrefixOptions = migrationConfigDto.getFullCheckSinkProcessJvm();
        String checkPrefixOptions = migrationConfigDto.getFullCheckCheckProcessJvm();

        sourceProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_FULL_SOURCE, taskWorkspace, sourceConfig,
                DataCheckerProcessType.SOURCE, sourcePrefixOptions, true);
        sinkProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_FULL_SINK, taskWorkspace, sinkConfig,
                DataCheckerProcessType.SINK, sinkPrefixOptions, true);
        checkProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_FULL_CHECK, taskWorkspace, checkConfig,
                DataCheckerProcessType.CHECK, checkPrefixOptions, true);
    }

    /**
     * Init incremental check processes
     */
    protected void initIncrementalProcess() {
        String sourcePrefixOptions = migrationConfigDto.getIncrementalCheckSourceProcessJvm();
        String sinkPrefixOptions = migrationConfigDto.getIncrementalCheckSinkProcessJvm();
        String checkPrefixOptions = migrationConfigDto.getIncrementalCheckCheckProcessJvm();

        sourceProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_INCREMENTAL_SOURCE, taskWorkspace, sourceConfig,
                DataCheckerProcessType.SOURCE, sourcePrefixOptions, false);
        sinkProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_INCREMENTAL_SINK, taskWorkspace, sinkConfig,
                DataCheckerProcessType.SINK, sinkPrefixOptions, false);
        checkProcess = new DataCheckerProcess(
                ProcessNameConstants.DATA_CHECKER_INCREMENTAL_CHECK, taskWorkspace, checkConfig,
                DataCheckerProcessType.CHECK, checkPrefixOptions, false);
    }

    /**
     * Config check process port
     */
    protected void configProcessPort() {
        try {
            int expectPort = 9000;
            int checkPort = PortUtils.getUsefulPort(expectPort);
            int sourcePort = PortUtils.getUsefulPort(checkPort + 1);
            int sinkPort = PortUtils.getUsefulPort(sourcePort + 1);
            String urlPrefix = "http://127.0.0.1:";
            String checkUrl = urlPrefix + checkPort;
            String sourceUrl = urlPrefix + sourcePort;
            String sinkUrl = urlPrefix + sinkPort;

            HashMap<String, Object> changeConfig = new HashMap<>();
            changeConfig.put(DataCheckerSourceConfig.CHECK_SERVER_URI, checkUrl);
            changeConfig.put(DataCheckerSourceConfig.SERVER_PORT, sourcePort);
            sourceConfig.changeConfig(changeConfig);

            changeConfig.clear();
            changeConfig.put(DataCheckerSinkConfig.CHECK_SERVER_URI, checkUrl);
            changeConfig.put(DataCheckerSinkConfig.SERVER_PORT, sinkPort);
            sinkConfig.changeConfig(changeConfig);

            changeConfig.clear();
            changeConfig.put(DataCheckerCheckConfig.CHECK_SOURCE_URI, sourceUrl);
            changeConfig.put(DataCheckerCheckConfig.CHECK_SINK_URI, sinkUrl);
            changeConfig.put(DataCheckerCheckConfig.SERVER_PORT, checkPort);
            checkConfig.changeConfig(changeConfig);
        } catch (SocketException e) {
            throw new MigrationException("Failed to get available port for data check process", e);
        }
    }

    /**
     * Check start sign
     *
     * @param signFilePath sign file path
     * @return true if start sign is found, false otherwise
     */
    protected boolean checkStartSign(String signFilePath) {
        int whileNumber = DataCheckerConstants.WAIT_PROCESS_START_MILLIS / 1000;
        while (whileNumber > 0) {
            try {
                String fileContents = FileUtils.readFileContents(signFilePath);
                String sourceStartSign = DataCheckerHelper.getProcessStartSign(DataCheckerProcessType.SOURCE);
                String sinkStartSign = DataCheckerHelper.getProcessStartSign(DataCheckerProcessType.SINK);
                String checkStartSign = DataCheckerHelper.getProcessStartSign(DataCheckerProcessType.CHECK);
                if (fileContents.contains(sourceStartSign) && fileContents.contains(sinkStartSign)
                        && fileContents.contains(checkStartSign)) {
                    return true;
                }
            } catch (IOException e) {
                LOGGER.trace("Get start sign failed, error:{}", e.getMessage());
            }

            whileNumber--;
            ThreadUtils.sleep(1000);
        }
        return false;
    }
}
