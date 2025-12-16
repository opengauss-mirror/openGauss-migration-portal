/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.ConnectAvroStandaloneConfig;
import org.opengauss.constants.config.DebeziumConnectLog4jConfig;
import org.opengauss.constants.config.DebeziumOpenGaussSinkConfig;
import org.opengauss.constants.config.DebeziumOpenGaussSourceConfig;
import org.opengauss.constants.config.DebeziumPgsqlSinkConfig;
import org.opengauss.constants.config.DebeziumPgsqlSourceConfig;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DebeziumProcessType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.OpenGaussUtils;
import org.opengauss.utils.StringUtils;
import org.opengauss.utils.ThreadUtils;
import org.opengauss.utils.TimeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * debezium pgsql migration config helper
 *
 * @since 2025/6/10
 */
public class DebeziumPgsqlMigrationConfigHelper {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumPgsqlMigrationConfigHelper.class);

    private static String slotName;

    private DebeziumPgsqlMigrationConfigHelper() {
    }

    /**
     * get pgsql incremental migration source process config map
     *
     * @param dto pgsql migration config dto
     * @param taskWorkspace task workspace
     * @return Map source process config map
     */
    public static Map<String, Object> incrementalSourceConfig(
            PgsqlMigrationConfigDto dto, TaskWorkspace taskWorkspace) {
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_HOSTNAME, dto.getPgsqlDatabaseIp());
        changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_PORT, dto.getPgsqlDatabasePort());
        changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_USER, dto.getPgsqlDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_PASSWORD, dto.getPgsqlDatabasePassword());
        }
        changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_NAME, dto.getPgsqlDatabaseName());
        changeParams.put(DebeziumPgsqlSourceConfig.SCHEMA_INCLUDE_LIST, dto.getPgsqlDatabaseSchemas());

        changeParams.put(DebeziumPgsqlSourceConfig.NAME, "pgsql_source_" + taskWorkspace.getId());
        String databaseServerName = generateIncrementalDatabaseServerName(taskWorkspace);
        changeParams.put(DebeziumPgsqlSourceConfig.DATABASE_SERVER_NAME, databaseServerName);
        changeParams.put(DebeziumPgsqlSourceConfig.TRANSFORMS_ROUTE_REGEX, "^" + databaseServerName + "(.*)");
        changeParams.put(DebeziumPgsqlSourceConfig.TRANSFORMS_ROUTE_REPLACEMENT,
                generateIncrementalKafkaTopic(taskWorkspace));
        changeParams.put(DebeziumPgsqlSourceConfig.COMMIT_PROCESS_WHILE_RUNNING, true);
        String processFilePath = generateIncrementalProcessFilePath(taskWorkspace);
        changeParams.put(DebeziumPgsqlSourceConfig.SOURCE_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumPgsqlSourceConfig.CREATE_COUNT_INFO_PATH, processFilePath);

        changeParams.put(DebeziumPgsqlSourceConfig.SLOT_DROP_ON_STOP, "false");
        changeParams.put(DebeziumPgsqlSourceConfig.MIGRATION_TYPE, "incremental");

        int majorPgsqlVersion = FullReplicatePgsqlMigrationConfigHelper.getMajorPgsqlVersion(dto);
        if (majorPgsqlVersion >= 11) {
            changeParams.put(DebeziumPgsqlSourceConfig.TRUNCATE_HANDLING_MODE, "include");
            changeParams.put(DebeziumPgsqlSourceConfig.PLUGIN_NAME, "pgoutput");
        } else if (majorPgsqlVersion == 10) {
            changeParams.put(DebeziumPgsqlSourceConfig.TRUNCATE_HANDLING_MODE, "skip");
            changeParams.put(DebeziumPgsqlSourceConfig.PLUGIN_NAME, "pgoutput");
        } else {
            changeParams.put(DebeziumPgsqlSourceConfig.TRUNCATE_HANDLING_MODE, "skip");
            changeParams.put(DebeziumPgsqlSourceConfig.PLUGIN_NAME, "wal2json");
        }

        return changeParams;
    }

    /**
     * get pgsql incremental migration source process delete key set
     *
     * @param dto pgsql migration config dto
     * @return Set delete key set
     */
    public static Set<String> incrementalSourceConfigDeleteKeySet(PgsqlMigrationConfigDto dto) {
        Set<String> deleteKeySet = new HashSet<>();
        deleteKeySet.add(DebeziumPgsqlSourceConfig.TABLE_INCLUDE_LIST);
        deleteKeySet.add(DebeziumPgsqlSourceConfig.SCHEMA_EXCLUDE_LIST);
        deleteKeySet.add(DebeziumPgsqlSourceConfig.TABLE_EXCLUDE_LIST);
        if (dto.isUseInteractivePassword()) {
            deleteKeySet.add(DebeziumPgsqlSourceConfig.DATABASE_PASSWORD);
        }
        return deleteKeySet;
    }

    /**
     * get pgsql incremental migration sink process config map
     *
     * @param dto pgsql migration config dto
     * @param taskWorkspace task workspace
     * @return Map sink process config map
     */
    public static Map<String, Object> incrementalSinkConfig(PgsqlMigrationConfigDto dto, TaskWorkspace taskWorkspace) {
        HashMap<String, Object> changeParams = new HashMap<>();

        changeParams.put(DebeziumPgsqlSinkConfig.DATABASE_USERNAME, dto.getOpengaussDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DebeziumPgsqlSinkConfig.DATABASE_PASSWORD, dto.getOpengaussDatabasePassword());
        }
        changeParams.put(DebeziumPgsqlSinkConfig.DATABASE_NAME, dto.getOpengaussDatabaseName());
        changeParams.put(DebeziumPgsqlSinkConfig.DATABASE_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(DebeziumPgsqlSinkConfig.DATABASE_IP, dto.getOpengaussDatabaseIp());

        Map<String, String> schemaMappings =
                FullReplicatePgsqlMigrationConfigHelper.getMigrationSchemaMappings(dto);
        StringBuilder mappingStrBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
            mappingStrBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }
        changeParams.put(DebeziumPgsqlSinkConfig.SCHEMA_MAPPINGS,
                mappingStrBuilder.substring(0, mappingStrBuilder.length() - 1));

        changeParams.put(DebeziumPgsqlSinkConfig.NAME, "pgsql_sink_" + taskWorkspace.getId());
        changeParams.put(DebeziumPgsqlSinkConfig.TOPICS, generateIncrementalKafkaTopic(taskWorkspace));
        changeParams.put(DebeziumPgsqlSinkConfig.COMMIT_PROCESS_WHILE_RUNNING, true);
        String processFilePath = generateIncrementalProcessFilePath(taskWorkspace);
        changeParams.put(DebeziumPgsqlSinkConfig.SINK_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumPgsqlSinkConfig.CREATE_COUNT_INFO_PATH, processFilePath);
        changeParams.put(DebeziumPgsqlSinkConfig.FAIL_SQL_PATH, processFilePath);

        String xlogPath = generateXlogPath(taskWorkspace);
        changeParams.put(DebeziumPgsqlSinkConfig.XLOG_LOCATION, xlogPath);
        return changeParams;
    }

    /**
     * get pgsql incremental migration sink process delete key set
     *
     * @param dto pgsql migration config dto
     * @return Set delete key set
     */
    public static Set<String> incrementalSinkConfigDeleteKeySet(PgsqlMigrationConfigDto dto) {
        Set<String> deleteKeySet = new HashSet<>();
        if (dto.isUseInteractivePassword()) {
            deleteKeySet.add(DebeziumPgsqlSinkConfig.DATABASE_PASSWORD);
        }
        return deleteKeySet;
    }

    /**
     * get pgsql incremental migration worker source process config map
     *
     * @param workspace task workspace
     * @return Map worker source process config map
     */
    public static Map<String, Object> incrementalWorkerSourceConfig(TaskWorkspace workspace) {
        Map<String, Object> changeParams = DebeziumMysqlMigrationConfigHelper.incrementalWorkerSourceConfig(workspace);

        changeParams.put(ConnectAvroStandaloneConfig.OFFSET_STORAGE_FILE_FILENAME,
                generateIncrementalStorageOffsetFilePath(workspace));
        return changeParams;
    }

    /**
     * get pgsql incremental migration worker sink process config map
     *
     * @param taskWorkspace task workspace
     * @return Map worker sink process config map
     */
    public static Map<String, Object> incrementalWorkerSinkConfig(TaskWorkspace taskWorkspace) {
        return incrementalWorkerSourceConfig(taskWorkspace);
    }

    /**
     * get pgsql incremental migration log4j config map
     *
     * @param workspace task workspace
     * @param processType process type
     * @return Map log4j config map
     */
    public static Map<String, Object> incrementalLog4jConfig(TaskWorkspace workspace, DebeziumProcessType processType) {
        Map<String, Object> changeParams =
                DebeziumMysqlMigrationConfigHelper.incrementalLog4jConfig(workspace, processType);
        String kafkaErrorLogPath = generateIncrementalKafkaErrorLogPath(workspace, processType);
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_FILE, kafkaErrorLogPath);
        return changeParams;
    }

    /**
     * get pgsql reverse migration source process config map
     *
     * @param dto pgsql migration config dto
     * @param taskWorkspace task workspace
     * @return Map source process config map
     */
    public static Map<String, Object> reverseSourceConfig(PgsqlMigrationConfigDto dto, TaskWorkspace taskWorkspace) {
        Map<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_HOSTNAME, dto.getOpengaussDatabaseIp());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_USER, dto.getOpengaussDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_PASSWORD, dto.getOpengaussDatabasePassword());
        }
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_NAME, dto.getOpengaussDatabaseName());

        Map<String, String> schemaMappings =
                FullReplicatePgsqlMigrationConfigHelper.getMigrationSchemaMappings(dto);
        StringBuilder includeSchemasBuilder = new StringBuilder();
        schemaMappings.forEach((key, value) -> includeSchemasBuilder.append(value).append(","));
        if (!StringUtils.isNullOrBlank(dto.getPgsqlDatabaseSchemas())) {
            changeParams.put(DebeziumOpenGaussSourceConfig.SCHEMA_INCLUDE_LIST,
                    includeSchemasBuilder.substring(0, includeSchemasBuilder.length() - 1));
        }

        String workspaceId = taskWorkspace.getId();
        changeParams.put(DebeziumOpenGaussSourceConfig.NAME, "opengauss_source_" + workspaceId);

        String databaseServerName = generateReverseDatabaseServerName(taskWorkspace);
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_SERVER_NAME, databaseServerName);
        changeParams.put(DebeziumOpenGaussSourceConfig.TRANSFORMS_ROUTE_REGEX, "^" + databaseServerName + "(.*)");
        changeParams.put(DebeziumOpenGaussSourceConfig.TRANSFORMS_ROUTE_REPLACEMENT,
                generateReverseKafkaTopic(taskWorkspace));

        String processFilePath = generateReverseProcessFilePath(taskWorkspace);
        changeParams.put(DebeziumOpenGaussSourceConfig.SOURCE_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumOpenGaussSourceConfig.CREATE_COUNT_INFO_PATH, processFilePath);

        changeParams.put(DebeziumOpenGaussSourceConfig.SLOT_NAME, generateReverseSlotName(taskWorkspace));
        changeParams.put(DebeziumOpenGaussSourceConfig.SLOT_DROP_ON_STOP, false);

        try (Connection connection = JdbcUtils.getOpengaussConnection(dto.getOpenGaussConnectInfo())) {
            if (!OpenGaussUtils.isSystemAdmin(dto.getOpengaussDatabaseUsername(), connection)) {
                changeParams.put(DebeziumOpenGaussSourceConfig.PUBLICATION_AUTO_CREATE_MODE, "filtered");
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to get system admin status, publication.autocreate.mode is not set to"
                    + " filtered. Error: {}", e.getMessage());
        }

        return changeParams;
    }

    /**
     * get pgsql reverse migration source process delete key set
     *
     * @param dto pgsql migration config dto
     * @return Set delete key set
     */
    public static Set<String> reverseSourceConfigDeleteKeySet(PgsqlMigrationConfigDto dto) {
        Set<String> deleteKeySet = new HashSet<>();
        if (dto.isUseInteractivePassword()) {
            deleteKeySet.add(DebeziumOpenGaussSourceConfig.DATABASE_PASSWORD);
        }
        return deleteKeySet;
    }

    /**
     * get pgsql reverse migration sink process config map
     *
     * @param dto pgsql migration config dto
     * @param taskWorkspace task workspace
     * @return Map sink process config map
     */
    public static Map<String, Object> reverseSinkConfig(PgsqlMigrationConfigDto dto, TaskWorkspace taskWorkspace) {
        Map<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_TYPE, "postgres");
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_IP, dto.getPgsqlDatabaseIp());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_PORT, dto.getPgsqlDatabasePort());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_USERNAME, dto.getPgsqlDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_PASSWORD, dto.getPgsqlDatabasePassword());
        }
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_NAME, dto.getPgsqlDatabaseName());

        Map<String, String> schemaMappings =
                FullReplicatePgsqlMigrationConfigHelper.getMigrationSchemaMappings(dto);
        StringBuilder mappingStrBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : schemaMappings.entrySet()) {
            mappingStrBuilder.append(entry.getValue()).append(":").append(entry.getKey()).append(";");
        }
        changeParams.put(DebeziumOpenGaussSinkConfig.SCHEMA_MAPPINGS,
                mappingStrBuilder.substring(0, mappingStrBuilder.length() - 1));

        String workspaceId = taskWorkspace.getId();
        changeParams.put(DebeziumOpenGaussSinkConfig.NAME, "opengauss_sink_" + workspaceId);
        changeParams.put(DebeziumOpenGaussSinkConfig.TOPICS, generateReverseKafkaTopic(taskWorkspace));
        changeParams.put(DebeziumOpenGaussSinkConfig.RECORD_BREAKPOINT_KAFKA_TOPIC,
                generateReverseBreakpointKafkaTopic(taskWorkspace));

        String processFilePath = generateReverseProcessFilePath(taskWorkspace);
        changeParams.put(DebeziumOpenGaussSinkConfig.CREATE_COUNT_INFO_PATH, processFilePath);
        changeParams.put(DebeziumOpenGaussSinkConfig.SINK_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumOpenGaussSinkConfig.FAIL_SQL_PATH, processFilePath);

        String kafkaServer = Kafka.getInstance().getKafkaIpPort();
        changeParams.put(DebeziumOpenGaussSinkConfig.RECORD_BREAKPOINT_KAFKA_BOOTSTRAP_SERVERS, kafkaServer);

        return changeParams;
    }

    /**
     * get pgsql reverse migration sink process delete key set
     *
     * @param dto pgsql migration config dto
     * @return Set delete key set
     */
    public static Set<String> reverseSinkConfigDeleteKeySet(PgsqlMigrationConfigDto dto) {
        Set<String> deleteKeySet = new HashSet<>();
        if (dto.isUseInteractivePassword()) {
            deleteKeySet.add(DebeziumOpenGaussSinkConfig.DATABASE_PASSWORD);
        }
        return deleteKeySet;
    }

    /**
     * get pgsql reverse migration worker source process config map
     *
     * @param taskWorkspace task workspace
     * @return Map worker source process config map
     */
    public static Map<String, Object> reverseWorkerSourceConfig(TaskWorkspace taskWorkspace) {
        Map<String, Object> changeParams = incrementalWorkerSourceConfig(taskWorkspace);
        changeParams.put(ConnectAvroStandaloneConfig.OFFSET_STORAGE_FILE_FILENAME,
                generateReverseStorageOffsetFilePath(taskWorkspace));
        return changeParams;
    }

    /**
     * get pgsql reverse migration worker sink process config map
     *
     * @param taskWorkspace task workspace
     * @return Map worker sink process config map
     */
    public static Map<String, Object> reverseWorkerSinkConfig(TaskWorkspace taskWorkspace) {
        return reverseWorkerSourceConfig(taskWorkspace);
    }

    /**
     * get pgsql reverse migration log4j config map
     *
     * @param workspace task workspace
     * @param processType process type
     * @return Map log4j config map
     */
    public static Map<String, Object> reverseLog4jConfig(TaskWorkspace workspace, DebeziumProcessType processType) {
        Map<String, Object> changeParams =
                DebeziumMysqlMigrationConfigHelper.reverseLog4jConfig(workspace, processType);
        String kafkaErrorLogPath = generateReverseKafkaErrorLogPath(workspace, processType);
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_FILE, kafkaErrorLogPath);
        return changeParams;
    }

    /**
     * get pgsql incremental migration slot name
     *
     * @param migrationConfigDto pgsql migration config dto
     * @param workspace task workspace
     * @return String slot name
     */
    public static synchronized String generateIncrementalSlotName(
            PgsqlMigrationConfigDto migrationConfigDto, TaskWorkspace workspace) {
        if (slotName == null) {
            slotName = "slot_" + workspace.getId();

            String selectSlotsSql = "SELECT * FROM pg_get_replication_slots();";
            try (Connection connection = JdbcUtils.getPgsqlConnection(migrationConfigDto.getPgsqlConnectInfo());
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(selectSlotsSql)) {
                ArrayList<String> slotList = new ArrayList<>();
                while (resultSet.next()) {
                    slotList.add(resultSet.getString("slot_name"));
                }

                while (slotList.contains(slotName)) {
                    slotName = slotName + "_" + TimeUtils.timestampFrom20250101();
                    ThreadUtils.sleep(10);
                }
            } catch (SQLException | ClassNotFoundException e) {
                throw new ConfigException("Failed to select pgsql replication slots", e);
            }
        }
        return slotName;
    }

    /**
     * get pgsql reverse migration slot name
     *
     * @param taskWorkspace task workspace
     * @return String slot name
     */
    public static String generateReverseSlotName(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateReverseSlotName(taskWorkspace);
    }

    /**
     * get pgsql incremental migration kafka topic
     *
     * @param taskWorkspace task workspace
     * @return String kafka topic
     */
    public static String generateIncrementalKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateIncrementalDatabaseServerName(taskWorkspace) + "_topic";
    }

    /**
     * get pgsql reverse migration kafka topic
     *
     * @param taskWorkspace task workspace
     * @return String kafka topic
     */
    public static String generateReverseKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateReverseDatabaseServerName(taskWorkspace) + "_topic";
    }

    /**
     * get pgsql reverse migration breakpoint kafka topic
     *
     * @param taskWorkspace task workspace
     * @return String breakpoint kafka topic
     */
    public static String generateReverseBreakpointKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateReverseKafkaTopic(taskWorkspace) + "_bp";
    }

    /**
     * get pgsql incremental migration connect kafka error log path
     *
     * @param taskWorkspace task workspace
     * @param processType process type
     * @return String connect kafka error log path
     */
    public static String generateIncrementalKafkaErrorLogPath(
            TaskWorkspace taskWorkspace, DebeziumProcessType processType) {
        return DebeziumMysqlMigrationConfigHelper.generateIncrementalKafkaErrorLogPath(taskWorkspace, processType);
    }

    /**
     * get pgsql reverse migration connect kafka error log path
     *
     * @param taskWorkspace task workspace
     * @param processType process type
     * @return String connect kafka error log path
     */
    public static String generateReverseKafkaErrorLogPath(
            TaskWorkspace taskWorkspace, DebeziumProcessType processType) {
        return DebeziumMysqlMigrationConfigHelper.generateReverseKafkaErrorLogPath(taskWorkspace, processType);
    }

    /**
     * get pgsql incremental migration storage offset file path
     *
     * @param taskWorkspace task workspace
     * @return String storage offset file path
     */
    public static String generateIncrementalStorageOffsetFilePath(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateIncrementalStorageOffsetFilePath(taskWorkspace);
    }

    /**
     * get pgsql reverse migration storage offset file path
     *
     * @param taskWorkspace task workspace
     * @return String storage offset file path
     */
    public static String generateReverseStorageOffsetFilePath(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateReverseStorageOffsetFilePath(taskWorkspace);
    }

    /**
     * get pgsql incremental migration process file path
     *
     * @param taskWorkspace task workspace
     * @return String process file path
     */
    public static String generateIncrementalProcessFilePath(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateIncrementalProcessFilePath(taskWorkspace);
    }

    /**
     * get pgsql reverse migration process file path
     *
     * @param taskWorkspace task workspace
     * @return String process file path
     */
    public static String generateReverseProcessFilePath(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateReverseProcessFilePath(taskWorkspace);
    }

    /**
     * Read xlog
     *
     * @param taskWorkspace task workspace
     * @return xlog
     */
    public static String readXlogLocation(TaskWorkspace taskWorkspace) {
        String xlogPath = generateXlogPath(taskWorkspace);
        String xlogLocation = "";
        try {
            String fileContents = FileUtils.readFileContents(xlogPath);
            String[] lines = fileContents.split("\n");
            for (String line : lines) {
                if (line.contains(DebeziumOpenGaussSourceConfig.XLOG_LOCATION)) {
                    int index = line.lastIndexOf("=") + 1;
                    xlogLocation = line.substring(index).trim();
                }
            }
        } catch (IOException ignored) {
            LOGGER.trace("Failed to read xlog from file: {}", xlogPath);
        }
        return xlogLocation;
    }

    private static String generateXlogPath(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateXlogPath(taskWorkspace);
    }

    private static String generateIncrementalDatabaseServerName(TaskWorkspace taskWorkspace) {
        return "pgsql_server_" + taskWorkspace.getId();
    }

    private static String generateReverseDatabaseServerName(TaskWorkspace taskWorkspace) {
        return DebeziumMysqlMigrationConfigHelper.generateReverseDatabaseServerName(taskWorkspace);
    }
}
