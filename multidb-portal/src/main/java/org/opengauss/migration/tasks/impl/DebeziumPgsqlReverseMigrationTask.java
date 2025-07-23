/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.DebeziumOpenGaussSourceConfig;
import org.opengauss.domain.dto.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.DebeziumConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.helper.config.DebeziumPgsqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.FullMigrationToolPgsqlMigrationConfigHelper;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.tasks.phase.ReverseMigrationTask;
import org.opengauss.migration.tasks.tool.DebeziumTask;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.OpenGaussUtils;
import org.opengauss.utils.PgsqlUtils;
import org.opengauss.utils.ThreadUtils;
import org.opengauss.utils.TimeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * debezium pgsql reverse migration task
 *
 * @since 2025/6/24
 */
public class DebeziumPgsqlReverseMigrationTask extends DebeziumTask implements ReverseMigrationTask {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumPgsqlReverseMigrationTask.class);
    private static final String PUBLICATION_NAME = "dbz_publication";

    private final PgsqlMigrationConfigDto migrationConfigDto;

    public DebeziumPgsqlReverseMigrationTask(
            ProcessMonitor processMonitor, MigrationStopIndicator migrationStopIndicator, TaskWorkspace taskWorkspace,
            PgsqlMigrationConfigDto migrationConfigDto, DebeziumConfigBundle debeziumConfig) {
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
        String xlogLocation = DebeziumPgsqlMigrationConfigHelper.readXlogLocation(taskWorkspace);
        changeConfig.put(DebeziumOpenGaussSourceConfig.XLOG_LOCATION, xlogLocation);
        sourceConnectConfig.changeConfig(changeConfig);
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

        try (Connection connection = JdbcUtils.getOpengaussConnection(migrationConfigDto.getOpenGaussConnectInfo())) {
            createLogicalReplicationSlot(connection);
            alterTableReplicaIdentityFull(connection);

            List<String> schemaTables = getSchemaTables(connection);
            createPublication(connection, schemaTables);
        } catch (SQLException e) {
            throw new MigrationException("Failed to connect to opengauss database", e);
        }

        checkKafkaStatus();
        cleanTopics();
        super.cleanHistoryProcess();
    }

    @Override
    public void afterTask() {
        cleanTopics();
        try (Connection connection = JdbcUtils.getOpengaussConnection(migrationConfigDto.getOpenGaussConnectInfo())) {
            alterTableReplicaIdentityDefault(connection);
            dropPublication(connection);
            dropLogicalReplicationSlot(connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to clean environment after reverse migration", e);
        }
    }

    private void cleanHistoryFiles() {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        String logsDirPath = taskWorkspace.getLogsReverseDirPath();
        String offsetDirPath = DebeziumPgsqlMigrationConfigHelper.generateReverseStorageOffsetFilePath(taskWorkspace);

        try {
            FileUtils.cleanDirectory(statusDirPath);
            FileUtils.cleanDirectory(logsDirPath);
            FileUtils.deletePath(offsetDirPath);
        } catch (IOException e) {
            throw new MigrationException("Failed to clean reverse migration history files", e);
        }
    }

    private List<String> getSchemaTables(Connection connection) {
        List<String> schemaTables = new ArrayList<>();
        try {
            Map<String, String> schemaMappings = FullMigrationToolPgsqlMigrationConfigHelper.getMigrationSchemaMappings(
                    migrationConfigDto);
            for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
                String targetSchema = entry.getValue();
                List<String> tables = PgsqlUtils.getSchemaTableNames(targetSchema, connection);
                for (String table : tables) {
                    schemaTables.add(targetSchema + "." + table);
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to get openGauss database schema tables", e);
        }
        return schemaTables;
    }

    private void createLogicalReplicationSlot(Connection connection) {
        try {
            List<String> slotList = PgsqlUtils.getReplicationSlotNames(connection);
            String slotName = DebeziumPgsqlMigrationConfigHelper.generateReverseSlotName(taskWorkspace);
            while (slotList.contains(slotName)) {
                slotName = slotName + "_" + TimeUtils.timestampFrom20250101();
                ThreadUtils.sleep(10);
            }

            String pluginName = sourceConnectConfig.getConfigMap().get(DebeziumOpenGaussSourceConfig.PLUGIN_NAME)
                    .toString();
            String pgOutputPluginName = "pgoutput";
            String mppdbDecodingPluginName = "mppdb_decoding";
            if (pgOutputPluginName.equals(pluginName) || mppdbDecodingPluginName.equals(pluginName)) {
                PgsqlUtils.createReplicationSlot(slotName, pluginName, connection);
            } else {
                throw new ConfigException("Unsupported plugin name: " + pluginName
                        + " in reverse migration source connector config");
            }
            changeSlotName(slotName);
        } catch (SQLException e) {
            throw new MigrationException("Failed to create logical replication slot", e);
        }
    }

    private void dropLogicalReplicationSlot(Connection connection) {
        String slotName = sourceConnectConfig.getConfigMap().get(DebeziumOpenGaussSourceConfig.SLOT_NAME).toString();
        try {
            List<String> slotList = PgsqlUtils.getReplicationSlotNames(connection);
            if (slotList.contains(slotName)) {
                PgsqlUtils.dropReplicationSlot(slotName, connection);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to drop logical replication slot: {}", slotName, e);
        }
    }

    private void createPublication(Connection connection, List<String> tableNames) {
        try (Statement statement = connection.createStatement()) {
            for (String publicationName : PgsqlUtils.getPublicationNames(connection)) {
                if (PUBLICATION_NAME.equals(publicationName)) {
                    PgsqlUtils.dropPublication(PUBLICATION_NAME, connection);
                    break;
                }
            }

            String createSql = String.format("CREATE PUBLICATION %s FOR ALL TABLES "
                    + "WITH(publish='insert,update,delete,truncate',ddl='all');", PUBLICATION_NAME);
            try {
                statement.execute(createSql);
            } catch (SQLException e) {
                try {
                    if (OpenGaussUtils.isSystemAdmin(migrationConfigDto.getOpengaussDatabaseUsername(), connection)) {
                        PgsqlUtils.createPublicationAllTables(PUBLICATION_NAME, connection);
                    } else {
                        PgsqlUtils.createPublicationForTable(PUBLICATION_NAME, tableNames, connection);
                    }
                } catch (SQLException ex) {
                    throw new MigrationException("Failed to create publication", ex);
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to select or drop publication", e);
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

    private void alterTableReplicaIdentityFull(Connection connection) {
        try {
            Map<String, String> schemaMappings = FullMigrationToolPgsqlMigrationConfigHelper.getMigrationSchemaMappings(
                    migrationConfigDto);
            for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
                String targetSchema = entry.getValue();
                List<String> tables = PgsqlUtils.getSchemaTableNames(targetSchema, connection);
                for (String table : tables) {
                    PgsqlUtils.alterTableReplicaIdentityFull(targetSchema, table, connection);
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to change tables replica identity to full, error: {}", e.getMessage());
        }
    }

    private void alterTableReplicaIdentityDefault(Connection connection) {
        try {
            Map<String, String> schemaMappings = FullMigrationToolPgsqlMigrationConfigHelper.getMigrationSchemaMappings(
                    migrationConfigDto);
            for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
                String targetSchema = entry.getValue();
                List<String> tables = PgsqlUtils.getSchemaTableNames(targetSchema, connection);
                for (String table : tables) {
                    PgsqlUtils.alterTableReplicaIdentityDefault(targetSchema, table, connection);
                }
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

        String kafkaTopic = DebeziumPgsqlMigrationConfigHelper.generateReverseKafkaTopic(taskWorkspace);
        String breakpointKafkaTopic = DebeziumPgsqlMigrationConfigHelper.generateReverseBreakpointKafkaTopic(
                taskWorkspace);
        String configKafkaTopic = String.format("config_%s", kafkaTopic);

        if (kafkaTopics.contains(kafkaTopic)) {
            kafka.deleteKafkaTopic(kafkaTopic);
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
