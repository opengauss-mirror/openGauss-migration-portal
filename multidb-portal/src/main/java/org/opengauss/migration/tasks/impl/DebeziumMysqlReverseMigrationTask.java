/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.DebeziumOpenGaussSourceConfig;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.model.DebeziumConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.migration.helper.config.DebeziumMysqlMigrationConfigHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.tasks.phase.ReverseMigrationTask;
import org.opengauss.migration.tasks.tool.DebeziumTask;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.OpenGaussUtils;
import org.opengauss.utils.StringUtils;
import org.opengauss.utils.ThreadUtils;
import org.opengauss.utils.TimeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

/**
 * Debezium mysql reverse migration task
 *
 * @since 2025/3/20
 */
public class DebeziumMysqlReverseMigrationTask extends DebeziumTask implements ReverseMigrationTask {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumMysqlReverseMigrationTask.class);
    private static final String PUBLICATION_NAME = "dbz_publication";

    private final MysqlMigrationConfigDto migrationConfigDto;

    public DebeziumMysqlReverseMigrationTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            MysqlMigrationConfigDto migrationConfigDto, DebeziumConfigBundle debeziumConfig) {
        super(processMonitor, migrationStopIndicator, taskWorkspace, debeziumConfig);
        this.migrationConfigDto = migrationConfigDto;
    }

    @Override
    protected DebeziumProcess generateSourceProcess() {
        String processJvm = migrationConfigDto.getReverseMigrationSourceProcessJvm();
        // is alert log collection is enabled, add alert config after the jvm config
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", processJvm);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_REVERSE_CONNECT_SOURCE, taskWorkspace,
                sourceConnectConfig, sourceWorkerConfig, sourceLog4jConfig, commandPrefix);
    }

    @Override
    protected DebeziumProcess generateSinkProcess() {
        String jvmPrefix = migrationConfigDto.getReverseMigrationSinkProcessJvm();
        // is alert log collection is enabled, add alert config after the jvm config
        String commandPrefix = String.format("export KAFKA_HEAP_OPTS=\"%s\"", jvmPrefix);
        return new DebeziumProcess(ProcessNameConstants.DEBEZIUM_REVERSE_CONNECT_SINK, taskWorkspace,
                sinkConnectConfig, sinkWorkerConfig, sinkLog4jConfig, commandPrefix);
    }

    @Override
    protected void beforeSourceProcess() {
        Kafka.getInstance().setSchemaCompatibilityToNone();

        setSourcePort();

        HashMap<String, Object> changeConfig = new HashMap<>();
        String xlogLocation = DebeziumMysqlMigrationConfigHelper.readXlogLocation(taskWorkspace);
        changeConfig.put(DebeziumOpenGaussSourceConfig.XLOG_LOCATION, xlogLocation);
        sourceConnectConfig.changeConfig(changeConfig);
    }

    @Override
    protected void beforeSinkProcess() {
        setSinkPort();
    }

    @Override
    public void beforeTask() {
        cleanHistoryFiles();

        try (Connection connection = JdbcUtils.getOpengaussConnection(migrationConfigDto.getOpenGaussConnectInfo())) {
            if (!(connection instanceof PgConnection)) {
                throw new IllegalArgumentException("Connection is not an instance of PgConnection");
            }

            PgConnection pgConnection = (PgConnection) connection;
            String schema = migrationConfigDto.getOpengaussDatabaseSchema();
            if (StringUtils.isNullOrBlank(schema)) {
                schema = migrationConfigDto.getMysqlDatabaseName();
            }
            pgConnection.setSchema(schema);

            List<String> tables = OpenGaussUtils.getSchemaTableNames(schema, connection);
            alterTableReplicaIdentityFull(pgConnection, tables);
            createLogicalReplicationSlot(pgConnection);
            createPublication(pgConnection, tables);
        } catch (SQLException e) {
            throw new MigrationException("Failed to prepare environment before reverse migration", e);
        }

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
        try (Connection connection = JdbcUtils.getOpengaussConnection(migrationConfigDto.getOpenGaussConnectInfo())) {
            if (!(connection instanceof PgConnection)) {
                throw new IllegalArgumentException("Connection is not an instance of PgConnection");
            }

            PgConnection pgConnection = (PgConnection) connection;
            String schema = migrationConfigDto.getOpengaussDatabaseSchema();
            if (StringUtils.isNullOrBlank(schema)) {
                schema = migrationConfigDto.getMysqlDatabaseName();
            }
            pgConnection.setSchema(schema);

            List<String> tables = OpenGaussUtils.getSchemaTableNames(schema, connection);
            alterTableReplicaIdentityDefault(pgConnection, tables);
            dropPublication(pgConnection);
            dropLogicalReplicationSlot(pgConnection);
        } catch (SQLException e) {
            LOGGER.error("Failed to clean environment after reverse migration", e);
        }
    }

    private void cleanHistoryFiles() {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        String logsDirPath = taskWorkspace.getLogsReverseDirPath();
        String offsetDirPath = DebeziumMysqlMigrationConfigHelper.generateReverseStorageOffsetFilePath(taskWorkspace);

        try {
            FileUtils.cleanDirectory(statusDirPath);
            FileUtils.cleanDirectory(logsDirPath);
            FileUtils.deletePath(offsetDirPath);
        } catch (IOException e) {
            throw new MigrationException("Failed to clean reverse migration history files", e);
        }
    }

    private void createLogicalReplicationSlot(PgConnection connection) {
        try {
            List<String> slotList = OpenGaussUtils.getReplicationSlotNames(connection);
            String slotName = DebeziumMysqlMigrationConfigHelper.generateReverseSlotName(taskWorkspace);
            while (slotList.contains(slotName)) {
                slotName = slotName + "_" + TimeUtils.timestampFrom20250101();
                ThreadUtils.sleep(10);
            }

            String pluginName = sourceConnectConfig.getConfigMap().get(DebeziumOpenGaussSourceConfig.PLUGIN_NAME)
                    .toString();
            String pgoutputPluginName = "pgoutput";
            String mppdbDecodingPluginName = "mppdb_decoding";
            if (pgoutputPluginName.equals(pluginName) || mppdbDecodingPluginName.equals(pluginName)) {
                OpenGaussUtils.createReplicationSlot(slotName, pluginName, connection);
            } else {
                throw new ConfigException("Unsupported plugin name: " + pluginName
                        + " in reverse migration source connector config");
            }
            changeSlotName(slotName);
        } catch (SQLException e) {
            throw new MigrationException("Failed to create logical replication slot", e);
        }
    }

    private void dropLogicalReplicationSlot(PgConnection connection) {
        String slotName = sourceConnectConfig.getConfigMap().get(DebeziumOpenGaussSourceConfig.SLOT_NAME).toString();
        try {
            List<String> slotList = OpenGaussUtils.getReplicationSlotNames(connection);
            if (slotList.contains(slotName)) {
                OpenGaussUtils.dropReplicationSlot(slotName, connection);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to drop logical replication slot: {}", slotName, e);
        }
    }

    private void createPublication(PgConnection connection, List<String> tableNames) {
        try {
            for (String publicationName : OpenGaussUtils.getPublicationNames(connection)) {
                if (PUBLICATION_NAME.equals(publicationName)) {
                    return;
                }
            }

            if (OpenGaussUtils.isSystemAdmin(migrationConfigDto.getOpengaussDatabaseUsername(), connection)) {
                OpenGaussUtils.createPublicationAllTables(PUBLICATION_NAME, connection);
            } else {
                OpenGaussUtils.createPublicationForTable(PUBLICATION_NAME, tableNames, connection);
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to create publication", e);
        }
    }

    private void dropPublication(PgConnection connection) {
        try {
            for (String publicationName : OpenGaussUtils.getPublicationNames(connection)) {
                if (PUBLICATION_NAME.equals(publicationName)) {
                    OpenGaussUtils.dropPublication(PUBLICATION_NAME, connection);
                    break;
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to drop publication: {},", PUBLICATION_NAME, e);
        }
    }

    private void alterTableReplicaIdentityFull(PgConnection connection, List<String> tableNames) {
        try {
            for (String tableName : tableNames) {
                OpenGaussUtils.alterTableReplicaIdentityFull(connection.getSchema(), tableName, connection);
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to change tables replica identity to full, error: {}", e.getMessage());
        }
    }

    private void alterTableReplicaIdentityDefault(PgConnection connection, List<String> tableNames) {
        try {
            for (String tableName : tableNames) {
                OpenGaussUtils.alterTableReplicaIdentityDefault(connection.getSchema(), tableName, connection);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to change tables replica identity to default", e);
        }
    }

    private void changeSlotName(String slotName) {
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumOpenGaussSourceConfig.SLOT_NAME, slotName);
        sourceConnectConfig.changeConfig(changeParams);
    }

    private void cleanTopics() {
        Kafka kafka = Kafka.getInstance();
        List<String> kafkaTopics = kafka.getKafkaTopics();

        String kafkaTopic = DebeziumMysqlMigrationConfigHelper.generateReverseKafkaTopic(taskWorkspace);
        String historyKafkaTopic = DebeziumMysqlMigrationConfigHelper.generateReverseHistoryKafkaTopic(taskWorkspace);
        String breakpointKafkaTopic = DebeziumMysqlMigrationConfigHelper.generateReverseBreakpointKafkaTopic(
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
            LOGGER.warn("Before starting reverse task, check for Kafka server is abnormal, restarting Kafka...");
            if (!Kafka.getInstance().restart()) {
                throw new MigrationException("Failed to restart Kafka before start reverse task");
            }
        }
    }
}
