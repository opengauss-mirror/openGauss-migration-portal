/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.ConnectAvroStandaloneConfig;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.DebeziumConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.tool.DebeziumHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.utils.PortUtils;
import org.opengauss.utils.ProcessUtils;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;

/**
 * debezium task
 *
 * @since 2025/3/20
 */
public abstract class DebeziumTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumTask.class);

    /**
     * Migration stop indicator
     */
    protected final MigrationStopIndicator migrationStopIndicator;

    /**
     * Process monitor
     */
    protected final ProcessMonitor processMonitor;

    /**
     * Debezium source connect config
     */
    protected final ConfigFile sourceConnectConfig;

    /**
     * Debezium sink connect config
     */
    protected final ConfigFile sinkConnectConfig;

    /**
     * Debezium source worker config
     */
    protected final ConfigFile sourceWorkerConfig;

    /**
     * Debezium sink worker config
     */
    protected final ConfigFile sinkWorkerConfig;

    /**
     * Debezium source log4j config
     */
    protected final ConfigFile sourceLog4jConfig;

    /**
     * Debezium sink log4j config
     */
    protected final ConfigFile sinkLog4jConfig;

    private DebeziumProcess sourceProcess;
    private DebeziumProcess sinkProcess;
    private int sourcePort = 8083;
    private int sinkPort = 8084;

    protected DebeziumTask(ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator,
                           TaskWorkspace taskWorkspace, DebeziumConfigBundle debeziumConfig) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.processMonitor = processMonitor;
        this.sourceConnectConfig = debeziumConfig.getConnectSourceConfigFile();
        this.sinkConnectConfig = debeziumConfig.getConnectSinkConfigFile();
        this.sourceWorkerConfig = debeziumConfig.getWorkerSourceConfigFile();
        this.sinkWorkerConfig = debeziumConfig.getWorkerSinkConfigFile();
        this.sourceLog4jConfig = debeziumConfig.getLog4jSourceConfigFile();
        this.sinkLog4jConfig = debeziumConfig.getLog4jSinkConfigFile();
    }

    /**
     * Generate source process
     *
     * @return source process
     */
    protected abstract DebeziumProcess generateSourceProcess();

    /**
     * Generate sink process
     *
     * @return sink process
     */
    protected abstract DebeziumProcess generateSinkProcess();

    /**
     * Before source process
     */
    protected abstract void beforeSourceProcess();

    /**
     * Before sink process
     */
    protected abstract void beforeSinkProcess();

    /**
     * Start source process
     */
    protected void startSourceProcess() {
        beforeSourceProcess();

        if (sourceProcess != null && sourceProcess.isAlive()) {
            return;
        }

        sourceProcess = generateSourceProcess();
        if (sourceProcess.isAlive()) {
            LOGGER.warn("Check history {} is running", sourceProcess.getProcessName());
            return;
        }

        if (!migrationStopIndicator.isStopped()) {
            sourceProcess.start();
            processMonitor.addProcess(sourceProcess);
        }
    }

    /**
     * Start sink process
     */
    protected void startSinkProcess() {
        beforeSinkProcess();

        if (sinkProcess != null && sinkProcess.isAlive()) {
            return;
        }

        sinkProcess = generateSinkProcess();
        if (sinkProcess.isAlive()) {
            LOGGER.warn("Check history {} is running", sinkProcess.getProcessName());
            return;
        }

        if (!migrationStopIndicator.isStopped()) {
            sinkProcess.start();
            processMonitor.addProcess(sinkProcess);
        }
    }

    /**
     * Stop source process
     */
    protected void stopSourceProcess() {
        if (sourceProcess != null) {
            sourceProcess.stop();
        }
    }

    /**
     * Stop sink process
     */
    protected void stopSinkProcess() {
        if (sinkProcess != null) {
            sinkProcess.stop();
        }
    }

    /**
     * Resume pause task
     */
    protected void resumeProcess() {
        if (sinkProcess != null && sinkProcess.isStopped()) {
            startSinkProcess();
        }
        if (sinkProcess != null && sourceProcess.isStopped()) {
            startSourceProcess();
        }
    }

    /**
     * Clean history files
     */
    protected void cleanHistoryProcess() {
        try {
            String sourceCheckCommand = DebeziumHelper.generateProcessCheckCommand(sourceConnectConfig,
                    sourceWorkerConfig);
            String sinkCheckCommand = DebeziumHelper.generateProcessCheckCommand(sinkConnectConfig, sinkWorkerConfig);
            ProcessUtils.killProcessByCommandSnippet(sourceCheckCommand, true);
            ProcessUtils.killProcessByCommandSnippet(sinkCheckCommand, true);
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Clean history process with error: {}", e.getMessage());
        }
    }

    /**
     * Set source process port
     */
    protected void setSourcePort() {
        int expectPort = sinkPort + 1;
        try {
            sourcePort = PortUtils.getUsefulPort(expectPort);
        } catch (SocketException e) {
            throw new MigrationException("Failed to get available port for source debezium process", e);
        }

        HashMap<String, Object> changeConfig = new HashMap<>();
        changeConfig.put(ConnectAvroStandaloneConfig.REST_PORT, sourcePort);
        sourceWorkerConfig.changeConfig(changeConfig);
    }

    /**
     * Set sink process port
     */
    protected void setSinkPort() {
        int expectPort = sourcePort + 1;
        try {
            sinkPort = PortUtils.getUsefulPort(expectPort);
        } catch (SocketException e) {
            throw new MigrationException("Failed to get available port for sink debezium process", e);
        }

        HashMap<String, Object> changeConfig = new HashMap<>();
        changeConfig.put(ConnectAvroStandaloneConfig.REST_PORT, sinkPort);
        sinkWorkerConfig.changeConfig(changeConfig);
    }
}
