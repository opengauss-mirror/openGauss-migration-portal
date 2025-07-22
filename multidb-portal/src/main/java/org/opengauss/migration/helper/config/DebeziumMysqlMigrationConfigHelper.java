/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.ConnectAvroStandaloneConfig;
import org.opengauss.constants.config.DebeziumConnectLog4jConfig;
import org.opengauss.constants.config.DebeziumMysqlSinkConfig;
import org.opengauss.constants.config.DebeziumMysqlSourceConfig;
import org.opengauss.constants.config.DebeziumOpenGaussSinkConfig;
import org.opengauss.constants.config.DebeziumOpenGaussSourceConfig;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DebeziumProcessType;
import org.opengauss.migration.tools.Debezium;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.OpenGaussUtils;
import org.opengauss.utils.StringUtils;
import org.opengauss.utils.TimeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * debezium mysql migration config helper
 *
 * @since 2025/5/7
 */
public class DebeziumMysqlMigrationConfigHelper {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumMysqlMigrationConfigHelper.class);

    private DebeziumMysqlMigrationConfigHelper() {
    }

    /**
     * get mysql incremental migration source process config
     *
     * @param dto mysql migration config dto
     * @param workspace task workspace
     * @return incremental source config
     */
    public static Map<String, Object> incrementalSourceConfig(MysqlMigrationConfigDto dto, TaskWorkspace workspace) {
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_HOSTNAME, dto.getMysqlDatabaseIp());
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_PORT, dto.getMysqlDatabasePort());
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_USER, dto.getMysqlDatabaseUsername());
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_PASSWORD, dto.getMysqlDatabasePassword());
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_INCLUDE_LIST, dto.getMysqlDatabaseName());
        if (!StringUtils.isNullOrBlank(dto.getMysqlDatabaseTables())) {
            changeParams.put(DebeziumMysqlSourceConfig.TABLE_INCLUDE_LIST, dto.getMysqlDatabaseTables());
        }

        String kafkaServer = Kafka.getInstance().getKafkaIpPort();
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_HISTORY_KAFKA_SERVERS, kafkaServer);
        changeParams.put(DebeziumMysqlSourceConfig.KAFKA_BOOTSTRAP_SERVERS, kafkaServer);

        String workspaceId = workspace.getId();
        changeParams.put(DebeziumMysqlSourceConfig.NAME, "mysql_source_" + workspaceId);

        String databaseServerName = generateIncrementalDatabaseServerName(workspace);
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_SERVER_NAME, databaseServerName);
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_SERVER_ID,
                String.valueOf(TimeUtils.timestampFrom20250101()));
        changeParams.put(DebeziumMysqlSourceConfig.DATABASE_HISTORY_KAFKA_TOPIC,
                generateIncrementalHistoryKafkaTopic(workspace));
        changeParams.put(DebeziumMysqlSourceConfig.TRANSFORMS_ROUTE_REGEX,
                "^" + databaseServerName + "(.*)");
        changeParams.put(DebeziumMysqlSourceConfig.TRANSFORMS_ROUTE_REPLACEMENT,
                generateIncrementalKafkaTopic(workspace));

        String processFilePath = generateIncrementalProcessFilePath(workspace);
        changeParams.put(DebeziumMysqlSourceConfig.SOURCE_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumMysqlSourceConfig.CREATE_COUNT_INFO_PATH, processFilePath);
        return changeParams;
    }

    /**
     * get mysql incremental migration sink process config
     *
     * @param dto mysql migration config dto
     * @param workspace task workspace
     * @return incremental sink config
     */
    public static Map<String, Object> incrementalSinkConfig(MysqlMigrationConfigDto dto, TaskWorkspace workspace) {
        HashMap<String, Object> changeParams = new HashMap<>();

        String opengaussUrl = String.format("jdbc:opengauss://%s:%s/%s?loggerLevel=OFF",
                dto.getOpengaussDatabaseIp(), dto.getOpengaussDatabasePort(), dto.getOpengaussDatabaseName());
        changeParams.put(DebeziumMysqlSinkConfig.OPENGAUSS_URL, opengaussUrl);
        changeParams.put(DebeziumMysqlSinkConfig.OPENGAUSS_USERNAME, dto.getOpengaussDatabaseUsername());
        changeParams.put(DebeziumMysqlSinkConfig.OPENGAUSS_PASSWORD, dto.getOpengaussDatabasePassword());

        String schemaMappings = generateIncrementalSchemaMappings(dto);
        changeParams.put(DebeziumMysqlSinkConfig.SCHEMA_MAPPINGS, schemaMappings);

        if (dto.isOpenGaussClusterAvailable()) {
            changeParams.put(DebeziumMysqlSinkConfig.OPENGAUSS_STANDBY_HOSTS, dto.getOpengaussDatabaseStandbyHosts());
            changeParams.put(DebeziumMysqlSinkConfig.OPENGAUSS_STANDBY_PORTS, dto.getOpengaussDatabaseStandbyPorts());
        }

        String kafkaServer = Kafka.getInstance().getKafkaIpPort();
        changeParams.put(DebeziumMysqlSinkConfig.RECORD_BREAKPOINT_KAFKA_BOOTSTRAP_SERVERS, kafkaServer);

        String workspaceId = workspace.getId();
        changeParams.put(DebeziumMysqlSinkConfig.NAME, "mysql_sink_" + workspaceId);
        changeParams.put(DebeziumMysqlSinkConfig.TOPICS, generateIncrementalKafkaTopic(workspace));
        changeParams.put(DebeziumMysqlSinkConfig.RECORD_BREAKPOINT_KAFKA_TOPIC,
                generateIncrementalBreakpointKafkaTopic(workspace));

        String processFilePath = generateIncrementalProcessFilePath(workspace);
        changeParams.put(DebeziumMysqlSinkConfig.CREATE_COUNT_INFO_PATH, processFilePath);
        changeParams.put(DebeziumMysqlSinkConfig.SINK_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumMysqlSinkConfig.FAIL_SQL_PATH, processFilePath);

        String xlogPath = generateXlogPath(workspace);
        changeParams.put(DebeziumMysqlSinkConfig.XLOG_LOCATION, xlogPath);

        return changeParams;
    }

    /**
     * get mysql incremental migration worker source process config
     *
     * @param taskWorkspace task workspace
     * @return incremental worker source config
     */
    public static Map<String, Object> incrementalWorkerSourceConfig(TaskWorkspace taskWorkspace) {
        HashMap<String, Object> changeParams = new HashMap<>();

        Kafka kafka = Kafka.getInstance();
        String kafkaServer = kafka.getKafkaIpPort();
        String schemaRegistryUrl = kafka.getSchemaRegistryUrl();
        changeParams.put(ConnectAvroStandaloneConfig.SCHEMA_REGISTRY_URL_FOR_KEY_CONVERTER, schemaRegistryUrl);
        changeParams.put(ConnectAvroStandaloneConfig.SCHEMA_REGISTRY_URL_FOR_VALUE_CONVERTER, schemaRegistryUrl);
        changeParams.put(ConnectAvroStandaloneConfig.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY, "All");
        changeParams.put(ConnectAvroStandaloneConfig.KAFKA_SERVERS, kafkaServer);

        changeParams.put(ConnectAvroStandaloneConfig.OFFSET_STORAGE_FILE_FILENAME,
                generateIncrementalStorageOffsetFilePath(taskWorkspace));
        String pluginPath = "share/java, " + Debezium.getInstance().getInstallDirPath();
        changeParams.put(ConnectAvroStandaloneConfig.PLUGIN_PATH, pluginPath);

        return changeParams;
    }

    /**
     * get mysql incremental migration worker sink process config
     *
     * @param taskWorkspace task workspace
     * @return incremental worker sink config
     */
    public static Map<String, Object> incrementalWorkerSinkConfig(TaskWorkspace taskWorkspace) {
        return incrementalWorkerSourceConfig(taskWorkspace);
    }

    /**
     * get mysql incremental migration log4j config map
     *
     * @param taskWorkspace task workspace
     * @param processType process type
     * @return incremental log4j config
     */
    public static Map<String, Object> incrementalLog4jConfig(
            TaskWorkspace taskWorkspace, DebeziumProcessType processType) {
        HashMap<String, Object> changeParams = new HashMap<>();
        String logsIncrementalDirPath = taskWorkspace.getLogsIncrementalDirPath();
        String logPath = String.format("%s/incremental-connect-%s.log", logsIncrementalDirPath, processType.getType());
        changeParams.put(DebeziumConnectLog4jConfig.CONNECT_APPENDER_FILE, logPath);

        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_LOGGER, "ERROR, kafkaErrorAppender");
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER, "org.apache.log4j.FileAppender");
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_LAYOUT, "org.apache.log4j.PatternLayout");
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_LAYOUT_CONVERSION_PATTERN,
                "%d{yyyy-MM-dd HH:mm:ss,SSS} [%t] %p %c:(%L) - %m%n");

        String kafkaErrorLogPath = generateIncrementalKafkaErrorLogPath(taskWorkspace, processType);
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_FILE, kafkaErrorLogPath);
        return changeParams;
    }

    /**
     * get mysql incremental migration source process config
     *
     * @param dto mysql migration config dto
     * @param workspace task workspace
     * @return incremental source config
     */
    public static Map<String, Object> reverseSourceConfig(MysqlMigrationConfigDto dto, TaskWorkspace workspace) {
        Map<String, Object> changeParams = new HashMap<>();

        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_HOSTNAME, dto.getOpengaussDatabaseIp());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_USER, dto.getOpengaussDatabaseUsername());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_PASSWORD, dto.getOpengaussDatabasePassword());
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_NAME, dto.getOpengaussDatabaseName());
        if (!StringUtils.isNullOrBlank(dto.getMysqlDatabaseTables())) {
            changeParams.put(DebeziumOpenGaussSourceConfig.TABLE_INCLUDE_LIST, dto.getMysqlDatabaseTables());
        }

        if (dto.isOpenGaussClusterAvailable()) {
            changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_IS_CLUSTER, true);
            changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_STANDBY_HOSTNAMES,
                    dto.getOpengaussDatabaseStandbyHosts());
            changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_STANDBY_PORTS,
                    dto.getOpengaussDatabaseStandbyPorts());
        }

        String workspaceId = workspace.getId();
        changeParams.put(DebeziumOpenGaussSourceConfig.NAME, "opengauss_source_" + workspaceId);

        String databaseServerName = generateReverseDatabaseServerName(workspace);
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_SERVER_NAME, databaseServerName);
        changeParams.put(DebeziumOpenGaussSourceConfig.DATABASE_HISTORY_KAFKA_TOPIC,
                generateReverseHistoryKafkaTopic(workspace));
        changeParams.put(DebeziumOpenGaussSourceConfig.TRANSFORMS_ROUTE_REGEX,
                "^" + databaseServerName + "(.*)");
        changeParams.put(DebeziumOpenGaussSourceConfig.TRANSFORMS_ROUTE_REPLACEMENT,
                generateReverseKafkaTopic(workspace));

        String processFilePath = generateReverseProcessFilePath(workspace);
        changeParams.put(DebeziumOpenGaussSourceConfig.SOURCE_PROCESS_FILE_PATH, processFilePath);
        changeParams.put(DebeziumOpenGaussSourceConfig.CREATE_COUNT_INFO_PATH,
                processFilePath);

        changeParams.put(DebeziumOpenGaussSourceConfig.SLOT_NAME, generateReverseSlotName(workspace));
        changeParams.put(DebeziumOpenGaussSourceConfig.SLOT_DROP_ON_STOP, false);

        try (Connection connection = JdbcUtils.getOpengaussConnection(dto.getOpenGaussConnectInfo())) {
            if (!OpenGaussUtils.isSystemAdmin(dto.getOpengaussDatabaseUsername(), connection)) {
                changeParams.put(DebeziumOpenGaussSourceConfig.PUBLICATION_AUTO_CREATE_MODE, "filtered");
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to get system admin status, publication.autocreate.mode is not set to filtered."
                    + " Error: {}", e.getMessage());
        }
        return changeParams;
    }

    /**
     * get mysql incremental migration sink process config
     *
     * @param dto mysql migration config dto
     * @param taskWorkspace task workspace
     * @return incremental sink config
     */
    public static Map<String, Object> reverseSinkConfig(MysqlMigrationConfigDto dto, TaskWorkspace taskWorkspace) {
        Map<String, Object> changeParams = new HashMap<>();

        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_TYPE, "mysql");
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_IP, dto.getMysqlDatabaseIp());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_PORT, dto.getMysqlDatabasePort());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_USERNAME, dto.getMysqlDatabaseUsername());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_PASSWORD, dto.getMysqlDatabasePassword());
        changeParams.put(DebeziumOpenGaussSinkConfig.DATABASE_NAME, dto.getMysqlDatabaseName());

        String schemaMappings = generateReverseSchemaMappings(dto);
        changeParams.put(DebeziumOpenGaussSinkConfig.SCHEMA_MAPPINGS, schemaMappings);
        if (!StringUtils.isNullOrBlank(dto.getMysqlDatabaseTables())) {
            changeParams.put(DebeziumOpenGaussSinkConfig.TABLE_INCLUDE_LIST, dto.getMysqlDatabaseTables());
        }

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
     * get mysql incremental migration worker source process config
     *
     * @param taskWorkspace task workspace
     * @return incremental worker source config
     */
    public static Map<String, Object> reverseWorkerSourceConfig(TaskWorkspace taskWorkspace) {
        Map<String, Object> changeParams = incrementalWorkerSourceConfig(taskWorkspace);
        changeParams.put(ConnectAvroStandaloneConfig.OFFSET_STORAGE_FILE_FILENAME,
                generateReverseStorageOffsetFilePath(taskWorkspace));
        return changeParams;
    }

    /**
     * get mysql incremental migration worker sink process config
     *
     * @param taskWorkspace task workspace
     * @return incremental worker sink config
     */
    public static Map<String, Object> reverseWorkerSinkConfig(TaskWorkspace taskWorkspace) {
        return reverseWorkerSourceConfig(taskWorkspace);
    }

    /**
     * get mysql incremental migration log4j config
     *
     * @param workspace task workspace
     * @param processType process type
     * @return incremental log4j config
     */
    public static Map<String, Object> reverseLog4jConfig(TaskWorkspace workspace, DebeziumProcessType processType) {
        Map<String, Object> changeParams = incrementalLog4jConfig(workspace, processType);

        String logsReverseDirPath = workspace.getLogsReverseDirPath();
        String logPath = String.format("%s/reverse-connect-%s.log", logsReverseDirPath, processType.getType());
        changeParams.put(DebeziumConnectLog4jConfig.CONNECT_APPENDER_FILE, logPath);

        String kafkaErrorLogPath = generateReverseKafkaErrorLogPath(workspace, processType);
        changeParams.put(DebeziumConnectLog4jConfig.KAFKA_ERROR_APPENDER_FILE, kafkaErrorLogPath);
        return changeParams;
    }

    /**
     * generate mysql reverse migration openGauss slot name
     *
     * @param taskWorkspace task workspace
     * @return reverse migration slot name
     */
    public static String generateReverseSlotName(TaskWorkspace taskWorkspace) {
        return "slot_" + taskWorkspace.getId();
    }

    /**
     * generate mysql incremental migration connect kafka error log path
     *
     * @param workspace task workspace
     * @param processType process type
     * @return incremental migration connect kafka error log path
     */
    public static String generateIncrementalKafkaErrorLogPath(
            TaskWorkspace workspace, DebeziumProcessType processType) {
        String logsIncrementalDirPath = workspace.getLogsIncrementalDirPath();
        return String.format("%s/kafka-connect/connect-%s-error.log", logsIncrementalDirPath, processType.getType());
    }

    /**
     * generate mysql reverse migration connect kafka error log path
     *
     * @param workspace task workspace
     * @param processType process type
     * @return reverse migration connect kafka error log path
     */
    public static String generateReverseKafkaErrorLogPath(TaskWorkspace workspace, DebeziumProcessType processType) {
        String logsReverseDirPath = workspace.getLogsReverseDirPath();
        return String.format("%s/kafka-connect/connect-%s-error.log", logsReverseDirPath, processType.getType());
    }

    /**
     * generate mysql incremental migration kafka topic
     *
     * @param taskWorkspace task workspace
     * @return incremental migration kafka topic
     */
    public static String generateIncrementalKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateIncrementalDatabaseServerName(taskWorkspace) + "_topic";
    }

    /**
     * generate mysql reverse migration kafka topic
     *
     * @param taskWorkspace task workspace
     * @return reverse migration kafka topic
     */
    public static String generateReverseKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateReverseDatabaseServerName(taskWorkspace) + "_topic";
    }

    /**
     * generate mysql incremental migration history kafka topic
     *
     * @param taskWorkspace task workspace
     * @return incremental migration history kafka topic
     */
    public static String generateIncrementalHistoryKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateIncrementalKafkaTopic(taskWorkspace) + "_history";
    }

    /**
     * generate mysql reverse migration history kafka topic
     *
     * @param taskWorkspace task workspace
     * @return reverse migration history kafka topic
     */
    public static String generateReverseHistoryKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateReverseKafkaTopic(taskWorkspace) + "_history";
    }

    /**
     * generate mysql incremental migration breakpoint kafka topic
     *
     * @param taskWorkspace task workspace
     * @return incremental migration breakpoint kafka topic
     */
    public static String generateIncrementalBreakpointKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateIncrementalKafkaTopic(taskWorkspace) + "_bp";
    }

    /**
     * generate mysql reverse migration breakpoint kafka topic
     *
     * @param taskWorkspace task workspace
     * @return reverse migration breakpoint kafka topic
     */
    public static String generateReverseBreakpointKafkaTopic(TaskWorkspace taskWorkspace) {
        return generateReverseKafkaTopic(taskWorkspace) + "_bp";
    }

    /**
     * generate mysql incremental migration storage offset file path
     *
     * @param taskWorkspace task workspace
     * @return incremental migration storage offset file path
     */
    public static String generateIncrementalStorageOffsetFilePath(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getTmpDirPath(), "incremental-connect.offsets");
    }

    /**
     * generate mysql reverse migration storage offset file path
     *
     * @param taskWorkspace task workspace
     * @return reverse migration storage offset file path
     */
    public static String generateReverseStorageOffsetFilePath(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getTmpDirPath(), "reverse-connect.offsets");
    }

    /**
     * set mysql incremental migration snapshot offset
     *
     * @param changeParams change params map
     * @param dto mysql migration config dto
     */
    public static void setSnapshotOffset(Map<String, Object> changeParams, MysqlMigrationConfigDto dto) {
        String mysqlActiveCheckSql = "show variables like 'read_only';";
        String mysqlActiveGtidSql = "show global variables like 'server_uuid';";
        String mysqlStandbyGtidSql = "show slave status;";
        String snapshotSchema = "sch_chameleon";
        String oGGtidSql = "select t_binlog_name,i_binlog_position,t_gtid_set from sch_chameleon.t_replica_batch;";

        try (Connection opengaussConnection = JdbcUtils.getOpengaussConnection(dto.getOpenGaussConnectInfo())) {
            if (!OpenGaussUtils.isSchemaExists(snapshotSchema, opengaussConnection)) {
                return;
            }

            try (Connection mysqlConnection = JdbcUtils.getMysqlConnection(dto.getMysqlConnectInfo());
                 Statement mysqlStatement1 = mysqlConnection.createStatement();
                 Statement mysqlStatement2 = mysqlConnection.createStatement();
                 Statement mysqlStatement3 = mysqlConnection.createStatement();
                 ResultSet mysqlActiveCheckResultSet = mysqlStatement1.executeQuery(mysqlActiveCheckSql);
                 ResultSet mysqlActiveGtidResultSet = mysqlStatement2.executeQuery(mysqlActiveGtidSql);
                 ResultSet mysqlStandbyGtidResultSet = mysqlStatement3.executeQuery(mysqlStandbyGtidSql);
                 Statement oGStatement = opengaussConnection.createStatement();
                 ResultSet oGGtidResultSet = oGStatement.executeQuery(oGGtidSql)) {
                String mysqlCurrentUuid = "";
                if (mysqlActiveCheckResultSet.next()) {
                    String mysqlActiveResult = mysqlActiveCheckResultSet.getString("Value");
                    if (mysqlActiveResult.equals("OFF")) {
                        if (mysqlActiveGtidResultSet.next()) {
                            mysqlCurrentUuid = mysqlActiveGtidResultSet.getString("Value");
                        }
                    } else {
                        if (mysqlStandbyGtidResultSet.next()) {
                            mysqlCurrentUuid = mysqlStandbyGtidResultSet.getString("Master_UUID");
                        }
                    }
                }

                if (oGGtidResultSet.next()) {
                    String tBinlogName = oGGtidResultSet.getString("t_binlog_name");
                    String iBinlogPosition = oGGtidResultSet.getString("i_binlog_position");
                    String tGtidSet = oGGtidResultSet.getString("t_gtid_set");

                    if (StringUtils.isNullOrBlank(tGtidSet)) {
                        LOGGER.warn("Mysql Execute_Gtid_Set is empty");
                        return;
                    }

                    String preGtidSet = getPreGtidSet(tGtidSet, mysqlCurrentUuid);
                    changeParams.put(DebeziumMysqlSourceConfig.SNAPSHOT_OFFSET_BINLOG_FILENAME, tBinlogName);
                    changeParams.put(DebeziumMysqlSourceConfig.SNAPSHOT_OFFSET_BINLOG_POSITION, iBinlogPosition);
                    changeParams.put(DebeziumMysqlSourceConfig.SNAPSHOT_OFFSET_GTID_SET, preGtidSet);
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            LOGGER.warn("Failed to load Mysql Execute_Gtid_Set", e);
        }
    }

    /**
     * generate mysql incremental migration process file path
     *
     * @param taskWorkspace task workspace
     * @return incremental migration process file path
     */
    public static String generateIncrementalProcessFilePath(TaskWorkspace taskWorkspace) {
        return taskWorkspace.getStatusIncrementalDirPath();
    }

    /**
     * generate mysql reverse migration process file path
     *
     * @param taskWorkspace task workspace
     * @return reverse migration process file path
     */
    public static String generateReverseProcessFilePath(TaskWorkspace taskWorkspace) {
        return taskWorkspace.getStatusReverseDirPath();
    }

    /**
     * read xlog location
     *
     * @param taskWorkspace task workspace
     * @return xlog location
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
        } catch (IOException e) {
            LOGGER.warn("Failed to read xlog location, error: {}", e.getMessage());
        }
        return xlogLocation;
    }

    /**
     * generate mysql incremental migration xlog file path
     *
     * @param taskWorkspace task workspace
     * @return xlog file path
     */
    public static String generateXlogPath(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getStatusIncrementalDirPath(), "xlog.txt");
    }

    /**
     * generate mysql reverse migration database server name
     *
     * @param taskWorkspace task workspace
     * @return reverse migration database server name
     */
    public static String generateReverseDatabaseServerName(TaskWorkspace taskWorkspace) {
        return "opengauss_server_" + taskWorkspace.getId();
    }

    private static String generateIncrementalDatabaseServerName(TaskWorkspace taskWorkspace) {
        return "mysql_server_" + taskWorkspace.getId();
    }

    private static String getPreGtidSet(String tGtidSet, String mysqlCurrentUuid) {
        StringBuilder newGtidSet = new StringBuilder();

        String[] gtidSetParts = tGtidSet.replaceAll(System.lineSeparator(), "").split(",");
        for (String part : gtidSetParts) {
            int uuidIndex = part.lastIndexOf(":");
            String uuid = part.substring(0, uuidIndex);
            int offsetIndex = part.lastIndexOf("-") + 1;

            if (uuid.equals(mysqlCurrentUuid) && (part.contains("-")) && offsetIndex > uuidIndex) {
                long offset = Long.parseLong(part.substring(offsetIndex));
                offset--;
                part = part.substring(0, offsetIndex) + offset;
            }
            newGtidSet.append(part).append(",");
        }

        return newGtidSet.substring(0, newGtidSet.length() - 1);
    }

    private static String generateIncrementalSchemaMappings(MysqlMigrationConfigDto migrationConfigDto) {
        String schemaMappings;
        if (StringUtils.isNullOrBlank(migrationConfigDto.getOpengaussDatabaseSchema())) {
            schemaMappings = String.format("%s:%s", migrationConfigDto.getMysqlDatabaseName(),
                    migrationConfigDto.getMysqlDatabaseName());
        } else {
            schemaMappings = String.format("%s:%s", migrationConfigDto.getMysqlDatabaseName(),
                    migrationConfigDto.getOpengaussDatabaseSchema());
        }
        return schemaMappings;
    }

    private static String generateReverseSchemaMappings(MysqlMigrationConfigDto migrationConfigDto) {
        String schemaMappings;
        if (StringUtils.isNullOrBlank(migrationConfigDto.getOpengaussDatabaseSchema())) {
            schemaMappings = String.format("%s:%s", migrationConfigDto.getMysqlDatabaseName(),
                    migrationConfigDto.getMysqlDatabaseName());
        } else {
            schemaMappings = String.format("%s:%s", migrationConfigDto.getOpengaussDatabaseSchema(),
                    migrationConfigDto.getMysqlDatabaseName());
        }
        return schemaMappings;
    }
}
