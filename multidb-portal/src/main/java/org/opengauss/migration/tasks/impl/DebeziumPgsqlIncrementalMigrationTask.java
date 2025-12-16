/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.DebeziumConfig;
import org.opengauss.constants.config.DebeziumPgsqlSourceConfig;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.domain.tool.config.DebeziumConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.config.DebeziumPgsqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.FullReplicatePgsqlMigrationConfigHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.tasks.phase.IncrementalMigrationTask;
import org.opengauss.migration.tasks.tool.DebeziumTask;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.PgsqlUtils;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * debezium pgsql incremental migration task
 *
 * @since 2025/6/24
 */
public class DebeziumPgsqlIncrementalMigrationTask extends DebeziumTask implements IncrementalMigrationTask {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumPgsqlIncrementalMigrationTask.class);
    private static final String PUBLICATION_NAME = "dbz_publication";

    private final PgsqlMigrationConfigDto migrationConfigDto;

    public DebeziumPgsqlIncrementalMigrationTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            PgsqlMigrationConfigDto migrationConfigDto, DebeziumConfigBundle debeziumConfig) {
        super(processMonitor, migrationStopIndicator, taskWorkspace, debeziumConfig);
        this.migrationConfigDto = migrationConfigDto;
    }

    @Override
    protected DebeziumProcess generateSourceProcess() {
        String processJvm = migrationConfigDto.getIncrementalMigrationSourceProcessJvm();
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", processJvm);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SOURCE, taskWorkspace,
                sourceConnectConfig, sourceWorkerConfig, sourceLog4jConfig, commandPrefix, generateSourceProcessEnv());
    }

    @Override
    protected DebeziumProcess generateSinkProcess() {
        String jvmPrefix = migrationConfigDto.getIncrementalMigrationSinkProcessJvm();
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", jvmPrefix);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SINK, taskWorkspace,
                sinkConnectConfig, sinkWorkerConfig, sinkLog4jConfig, commandPrefix, generateSinkProcessEnv());
    }

    @Override
    protected void beforeSourceProcess() {
        Kafka.getInstance().setSchemaCompatibilityToNone();
        setSourcePort();

        String slotName = DebeziumPgsqlMigrationConfigHelper.generateIncrementalSlotName(migrationConfigDto,
                taskWorkspace);
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumPgsqlSourceConfig.SLOT_NAME, slotName);
        sourceConnectConfig.changeConfig(changeParams);
    }

    @Override
    protected void beforeSinkProcess() {
        setSinkPort();
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
    public void beforeTask() {
        cleanHistoryFiles();
        checkKafkaStatus();
        cleanTopics();
        super.cleanHistoryProcess();
    }

    @Override
    public void afterTask() {
        cleanTopics();
        try (Connection connection = JdbcUtils.getPgsqlConnection(migrationConfigDto.getPgsqlConnectInfo())) {
            if (!(connection instanceof PgConnection)) {
                throw new IllegalArgumentException("Connection is not a PgConnection");
            }

            PgConnection pgConnection = (PgConnection) connection;
            dropSlot(pgConnection);
            dropPublication(pgConnection);
            alterTableReplicaIdentityDefault(pgConnection);
        } catch (SQLException | ClassNotFoundException e) {
            LOGGER.error("Failed to clean environment after incremental migration", e);
        }
    }

    private void alterTableReplicaIdentityDefault(PgConnection connection) {
        try {
            Map<String, String> schemaMappings = FullReplicatePgsqlMigrationConfigHelper.getMigrationSchemaMappings(
                    migrationConfigDto);
            for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
                String sourceSchema = entry.getKey();
                List<String> tables = PgsqlUtils.getSchemaTableNames(sourceSchema, connection);
                for (String table : tables) {
                    PgsqlUtils.alterTableReplicaIdentityDefault(sourceSchema, table, connection);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to change tables replica identity to default", e);
        }
    }

    private void dropSlot(Connection connection) {
        String slotName = DebeziumPgsqlMigrationConfigHelper.generateIncrementalSlotName(migrationConfigDto,
                taskWorkspace);
        try {
            List<String> slotList = PgsqlUtils.getReplicationSlotNames(connection);
            if (slotList.contains(slotName)) {
                PgsqlUtils.dropReplicationSlot(slotName, connection);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to drop PostgreSQL logical replication slot: {}", slotName, e);
        }
    }

    private void dropPublication(Connection connection) {
        try {
            for (String publicationName : PgsqlUtils.getPublicationNames(connection)) {
                if (PUBLICATION_NAME.equals(publicationName)) {
                    PgsqlUtils.dropPublication(PUBLICATION_NAME, connection);
                    break;
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to drop publication: {}", PUBLICATION_NAME, e);
        }
    }

    private void cleanHistoryFiles() {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        String logsDirPath = taskWorkspace.getLogsIncrementalDirPath();
        String offsetDirPath = DebeziumPgsqlMigrationConfigHelper.generateIncrementalStorageOffsetFilePath(
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

        String kafkaTopic = DebeziumPgsqlMigrationConfigHelper.generateIncrementalKafkaTopic(taskWorkspace);
        String configKafkaTopic = String.format("config_%s", kafkaTopic);

        if (kafkaTopics.contains(kafkaTopic)) {
            kafka.deleteKafkaTopic(kafkaTopic);
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
            env.put(DebeziumConfig.ENV_DATABASE_PASSWORD, migrationConfigDto.getPgsqlDatabasePassword());
        }
        return env;
    }
}
