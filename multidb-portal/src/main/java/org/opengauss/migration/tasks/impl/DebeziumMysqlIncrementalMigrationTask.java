/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.DebeziumConfig;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.domain.tool.config.DebeziumConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.config.DebeziumMysqlMigrationConfigHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.tasks.phase.IncrementalMigrationTask;
import org.opengauss.migration.tasks.tool.DebeziumTask;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Debezium mysql incremental migration task
 *
 * @since 2025/3/20
 */
public class DebeziumMysqlIncrementalMigrationTask extends DebeziumTask implements IncrementalMigrationTask {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumMysqlIncrementalMigrationTask.class);
    private final MysqlMigrationConfigDto migrationConfigDto;

    public DebeziumMysqlIncrementalMigrationTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            MysqlMigrationConfigDto migrationConfigDto, DebeziumConfigBundle debeziumConfig) {
        super(processMonitor, migrationStopIndicator, taskWorkspace, debeziumConfig);
        this.migrationConfigDto = migrationConfigDto;
    }

    @Override
    protected DebeziumProcess generateSourceProcess() {
        String processJvm = migrationConfigDto.getIncrementalMigrationSourceProcessJvm();
        // is alert log collection is enabled, add alert config after the jvm config
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", processJvm);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SOURCE, taskWorkspace,
                sourceConnectConfig, sourceWorkerConfig, sourceLog4jConfig, commandPrefix, generateSourceProcessEnv());
    }

    @Override
    protected DebeziumProcess generateSinkProcess() {
        String jvmPrefix = migrationConfigDto.getIncrementalMigrationSinkProcessJvm();
        // is alert log collection is enabled, add alert config after the jvm config
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", jvmPrefix);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SINK, taskWorkspace,
                sinkConnectConfig, sinkWorkerConfig, sinkLog4jConfig, commandPrefix, generateSinkProcessEnv());
    }

    @Override
    protected void beforeSourceProcess() {
        Kafka.getInstance().setSchemaCompatibilityToNone();

        setSourcePort();

        HashMap<String, Object> changeConfig = new HashMap<>();
        DebeziumMysqlMigrationConfigHelper.setSnapshotOffset(changeConfig, migrationConfigDto);
        sourceConnectConfig.changeConfig(changeConfig);
    }

    @Override
    protected void beforeSinkProcess() {
        setSinkPort();
    }

    @Override
    public void beforeTask() {
        cleanHistoryFiles();
        checkKafkaStatus();
        cleanTopics();
        super.cleanHistoryProcess();
    }

    @Override
    public void startSource() {
        super.startSourceProcess();
    }

    @Override
    public void startSink() {
        super.startSinkProcess();
    }

    @Override
    public void stopSource() {
        super.stopSourceProcess();
    }

    @Override
    public void stopSink() {
        super.stopSinkProcess();
    }

    @Override
    public void resumeTask() {
        super.resumeProcess();
    }

    @Override
    public void afterTask() {
        cleanTopics();
    }

    private void cleanHistoryFiles() {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        String logsDirPath = taskWorkspace.getLogsIncrementalDirPath();
        String offsetDirPath = DebeziumMysqlMigrationConfigHelper.generateIncrementalStorageOffsetFilePath(
                taskWorkspace);

        try {
            FileUtils.cleanDirectory(statusDirPath);
            FileUtils.cleanDirectory(logsDirPath);
            FileUtils.deletePath(offsetDirPath);
        } catch (IOException e) {
            throw new MigrationException("Failed to clean incremental migration history files", e);
        }
    }

    private void cleanTopics() {
        Kafka kafka = Kafka.getInstance();
        List<String> kafkaTopics = kafka.getKafkaTopics();

        String kafkaTopic = DebeziumMysqlMigrationConfigHelper.generateIncrementalKafkaTopic(taskWorkspace);
        String historyKafkaTopic = DebeziumMysqlMigrationConfigHelper.generateIncrementalHistoryKafkaTopic(
                taskWorkspace);
        String breakpointKafkaTopic = DebeziumMysqlMigrationConfigHelper.generateIncrementalBreakpointKafkaTopic(
                taskWorkspace);
        String configKafkaTopic = String.format("config_%s", kafkaTopic);

        if (kafkaTopics.contains(kafkaTopic)) {
            kafka.deleteKafkaTopic(kafkaTopic);
        }
        if (kafkaTopics.contains(historyKafkaTopic)) {
            kafka.deleteKafkaTopic(historyKafkaTopic);
        }
        if (kafkaTopics.contains(breakpointKafkaTopic)) {
            kafka.deleteKafkaTopic(breakpointKafkaTopic);
        }
        if (kafkaTopics.contains(configKafkaTopic)) {
            kafka.deleteKafkaTopic(configKafkaTopic);
        }
    }

    private void checkKafkaStatus() {
        Kafka kafka = Kafka.getInstance();
        if (!kafka.status()) {
            LOGGER.warn("Before starting incremental task, check for Kafka server is abnormal, restarting Kafka...");
            if (!Kafka.getInstance().restart()) {
                throw new MigrationException("Failed to restart Kafka before start incremental task");
            }
        }
    }

    private Map<String, String> generateSinkProcessEnv() {
        Map<String, String> env = new HashMap<>();
        if (migrationConfigDto.isUseInteractivePassword()) {
            env.put(DebeziumConfig.ENABLE_ENV_PASSWORD, "true");
            env.put(DebeziumConfig.ENV_DATABASE_PASSWORD, migrationConfigDto.getOpengaussDatabasePassword());
        }
        return env;
    }

    private Map<String, String> generateSourceProcessEnv() {
        Map<String, String> env = new HashMap<>();
        if (migrationConfigDto.isUseInteractivePassword()) {
            env.put(DebeziumConfig.ENABLE_ENV_PASSWORD, "true");
            env.put(DebeziumConfig.ENV_DATABASE_PASSWORD, migrationConfigDto.getMysqlDatabasePassword());
        }
        return env;
    }
}
