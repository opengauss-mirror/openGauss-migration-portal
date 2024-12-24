/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.tools.mysql;

import org.apache.logging.log4j.util.Strings;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Offset;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.enums.ToolsConfigEnum;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.ConnectorMysql;
import org.opengauss.portalcontroller.software.Software;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;
import static org.opengauss.portalcontroller.constant.Debezium.Connector.LOG_PATTERN_PATH;
import static org.opengauss.portalcontroller.utils.ProcessUtils.checkProcess;

/**
 * IncrementalMigrationTool
 *
 * @date :2023/11/3 15:22
 * @description: IncrementalMigrationTool
 * @version: 1.1
 * @since 1.1
 */
public class IncrementalMigrationTool extends ParamsConfig implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalMigrationTool.class);
    private final LogFileListener incrementalLogFileListener = new LogFileListener();

    private final MysqlFullMigrationTool fullMigrationTool = new MysqlFullMigrationTool();

    Map<String, Object> sourceMap = null;
    Map<String, Object> sinkMap = null;

    Map<String, Object> sourceConnectMap = null;
    Map<String, Object> sinkConnectMap = null;

    Map<String, Object> logMap = null;

    /**
     * Change incremental migration parameters.
     */
    @Override
    public void initDataBaseParams() {
        String mysqlDatabaseHost = toolsMigrationParametersTable.get(Mysql.DATABASE_HOST);
        sourceMap.put(Debezium.Source.HOST, mysqlDatabaseHost);
        String mysqlDatabasePort = toolsMigrationParametersTable.get(Mysql.DATABASE_PORT);
        sourceMap.put(Debezium.Source.PORT, mysqlDatabasePort);
        String mysqlUserName = toolsMigrationParametersTable.get(Mysql.USER);
        sourceMap.put(Debezium.Source.USER, mysqlUserName);
        String mysqlUserPassword = toolsMigrationParametersTable.get(Mysql.PASSWORD);
        sourceMap.put(Debezium.Source.PASSWORD, mysqlUserPassword);
        String mysqlDatabaseName = toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
        sourceMap.put(Debezium.Source.WHITELIST, mysqlDatabaseName);
        setSourceTables();
        String openGaussSchemaName = toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA);
        sinkMap.put(Debezium.Sink.SCHEMA_MAPPING, mysqlDatabaseName + ":" + openGaussSchemaName);
        String opengaussUserName = toolsMigrationParametersTable.get(Opengauss.USER);
        sinkMap.put(Debezium.Sink.Opengauss.USER, opengaussUserName);
        String opengaussUserPassword = toolsMigrationParametersTable.get(Opengauss.PASSWORD);
        sinkMap.put(Debezium.Sink.Opengauss.PASSWORD, opengaussUserPassword);
        String opengaussDatabaseHost = toolsMigrationParametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = toolsMigrationParametersTable.get(Opengauss.DATABASE_PORT);
        String opengaussDatabaseName = toolsMigrationParametersTable.get(Opengauss.DATABASE_NAME);
        String opengaussDebeziumUrl =
                "jdbc:opengauss://" + opengaussDatabaseHost + ":" + opengaussDatabasePort + "/" + opengaussDatabaseName + "?loggerLevel=OFF";
        sinkMap.put(Debezium.Sink.Opengauss.URL, opengaussDebeziumUrl);

        if (Opengauss.isOpengaussClusterAvailable()) {
            sinkMap.put(Debezium.Sink.Opengauss.STANDBY_HOSTS,
                    toolsMigrationParametersTable.get(Opengauss.DATABASE_STANDBY_HOSTS));
            sinkMap.put(Debezium.Sink.Opengauss.STANDBY_PORTS,
                    toolsMigrationParametersTable.get(Opengauss.DATABASE_STANDBY_PORTS));
        }
    }

    /**
     * set database tables.
     */
    private void setSourceTables() {
        String tables = toolsMigrationParametersTable.get(Mysql.DATABASE_TABLE);
        if (!Plan.isRuleEnable(tables)) {
            return;
        }
        sourceMap.put(Debezium.Source.TABLELIST, tables);
    }

    /**
     * Find offset.
     *
     * @throws PortalException the portal exception
     */
    private static void findOffset() throws PortalException {
        LOGGER.info("Find snapshot for full and incremental migration.");
        Hashtable<String, String> offsetHashtable = new Hashtable<>();
        offsetHashtable.put(Offset.FILE, "");
        offsetHashtable.put(Offset.POSITION, "0");
        offsetHashtable.put(Offset.GTID, "");
        String sql = "select t_binlog_name,i_binlog_position,t_gtid_set from sch_chameleon.t_replica_batch;";
        try (
                Connection mysqlConnection = JdbcUtils.getMysqlConnection();
                ResultSet rs = JdbcUtils.getPgConnection().execSQLQuery(sql)
        ) {
            String uuid = JdbcUtils.getCurrentUuid(mysqlConnection);
            LOGGER.info("Current uuid: {}", uuid);
            if (rs.next()) {
                String tBinlogName = rs.getString("t_binlog_name");
                String iBinlogPosition = rs.getString("i_binlog_position");
                String tGtidSet = rs.getString("t_gtid_set");
                String offsetGtidSet = changeGtidSet(tGtidSet, uuid);
                offsetHashtable.put(Offset.FILE, tBinlogName);
                offsetHashtable.put(Offset.POSITION, iBinlogPosition);
                offsetHashtable.put(Offset.GTID, offsetGtidSet);
                LOGGER.info("Find snapshot from mysql full migration, file: {}, position: {}, gitd: {}.",
                        tBinlogName, iBinlogPosition, tGtidSet);
            }
        } catch (SQLException e) {
            LOGGER.warn("Schema sch_chameleon does not exists, use snapshot when incremental migration start.");
        }
        String offsetPath = toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH);
        PropertitesUtils.changePropertiesParameters(offsetHashtable, offsetPath);
    }

    /**
     * Change gtid set string.
     *
     * @param oldGtidSet the old gtid set
     * @param mysqlUuid  the mysql uuid
     * @return the string
     */
    public static String changeGtidSet(String oldGtidSet, String mysqlUuid) {
        StringBuilder newGtidSet = new StringBuilder();
        String[] gtidSetParts = oldGtidSet.replaceAll(System.lineSeparator(), "").split(",");
        for (String tGtidSet : gtidSetParts) {
            int uuidIndex = tGtidSet.lastIndexOf(":");
            String uuid = tGtidSet.substring(0, uuidIndex);
            int offsetIndex = tGtidSet.lastIndexOf("-") + 1;
            if (uuid.equals(mysqlUuid) && (tGtidSet.contains("-")) && offsetIndex > uuidIndex) {
                long offset = Long.parseLong(tGtidSet.substring(offsetIndex));
                LOGGER.info("Offset: {}", offset);
                offset--;
                tGtidSet = tGtidSet.substring(0, offsetIndex) + offset;
            }
            newGtidSet.append(tGtidSet).append(",");
        }
        newGtidSet = new StringBuilder(newGtidSet.substring(0, newGtidSet.length() - 1));
        return newGtidSet.toString();
    }

    /**
     * confluent params kafka zk register IP port
     */
    @Override
    public void initKafkaParams() {
        // connect-avro-standalone.properties 文件修改
        MigrationConfluentInstanceConfig portalConfig = MigrationConfluentInstanceConfig.getInstanceFromPortalConfig();
        Hashtable<String, String> connectAvroStandalonePropChangeParam = new Hashtable<>();
        String schemaRegistryPrefix = "http://";
        connectAvroStandalonePropChangeParam.put("bootstrap.servers", portalConfig.getKafkaIpPort());
        connectAvroStandalonePropChangeParam.put("key.converter.schema.registry.url",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        connectAvroStandalonePropChangeParam.put("value.converter.schema.registry.url",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        connectAvroStandalonePropChangeParam.put("connector.client.config.override.policy", "All");
        sourceConnectMap.putAll(connectAvroStandalonePropChangeParam);
        sinkConnectMap.putAll(connectAvroStandalonePropChangeParam);
        // mysql-sink.properties文件修改
        sinkMap.put("record.breakpoint.kafka.bootstrap.servers", portalConfig.getKafkaIpPort());
        // mysql-source.properties文件修改
        sourceMap.put("database.history.kafka.bootstrap.servers", portalConfig.getKafkaIpPort());
        sourceMap.put("kafka.bootstrap.server", portalConfig.getKafkaIpPort());
    }

    /**
     * install
     *
     * @param isDownload isDownload
     * @return boolean
     */
    @Override
    public boolean install(boolean isDownload) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new ConnectorMysql());
        InstallMigrationUtils installMigrationUtils = new InstallMigrationUtils();
        for (Software software : softwareArrayList) {
            try {
                installMigrationUtils.installSingleMigrationSoftware(software, isDownload);
            } catch (PortalException e) {
                LOGGER.error("install failed", e);
                return false;
            }
        }
        LogViewUtils.outputResult(true, Command.Install.Mysql.IncrementalMigration.DEFAULT);
        return true;
    }

    /**
     * initWorkSpaceParams
     *
     * @param workspaceId  workspaceId
     */
    @Override
    public void initWorkSpaceParams(String workspaceId) {
        sourceMap.put("name", "mysql-source-" + workspaceId);
        sourceMap.put("database.server.name", "mysql_server_" + workspaceId);
        sourceMap.put("database.server.id", String.valueOf(ProcessUtils.getCurrentPortalPid()));
        sourceMap.put("database.history.kafka.topic", "mysql_server_" + workspaceId + "_history");
        sourceMap.put("transforms.route.regex", "^" + "mysql_server_" + workspaceId + "(.*)");
        sourceMap.put("transforms.route.replacement", "mysql_server_" + workspaceId + "_topic");
        String incrementalFolder = toolsConfigParametersTable.get(Status.INCREMENTAL_FOLDER);
        sourceMap.put("source.process.file.path", incrementalFolder);
        sourceMap.put("create.count.info.path", incrementalFolder);
        sinkMap.put("name", "mysql-sink-" + workspaceId);
        sinkMap.put("topics", "mysql_server_" + workspaceId + "_topic");
        sinkMap.put("record.breakpoint.kafka.topic", "mysql_bp_" + workspaceId + "_topic");
        try {
            FileUtils.createFile(incrementalFolder, false);
        } catch (PortalException e) {
            e.setRequestInformation("Create incremental migration folder status folder failed.Please ensure the "
                    + "config folder " + incrementalFolder + " is available");
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, e.toString());
            return;
        }
        sinkMap.put("sink.process.file.path", incrementalFolder);
        sinkMap.put("create.count.info.path", incrementalFolder);

        sinkMap.put("fail.sql.path", incrementalFolder);
        if (Strings.isNotBlank(toolsMigrationParametersTable
                .get(MigrationParameters.Log.GLOBAL_LOG_LEVEL))) {
            logMap.put("log4j.rootLogger",
                    toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL)
                            .toUpperCase() + ", stdout, connectAppender");
        }
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        KafkaUtils.changekafkaLogParam(workspaceId + "_source", hashtable.get(LOG_PATTERN_PATH));
        int sourcePort = StartPort.REST_MYSQL_SOURCE + PortalControl.portId * 10;
        int port = ParamsUtils.getAvailablePorts(sourcePort, 1, 1000).get(0);
        sourceConnectMap.put("rest.port", String.valueOf(port));
    }

    /**
     *
     * initInteractionParams
     */
    @Override
    void initInteractionParams() {
        if (toolsMigrationParametersTable.containsKey(Offset.FILE)) {
            sourceMap.put(Offset.FILE, toolsMigrationParametersTable.get(Offset.FILE));
        }
        if (toolsMigrationParametersTable.containsKey(Offset.POSITION)) {
            sourceMap.put(Offset.POSITION, toolsMigrationParametersTable.get(Offset.POSITION));
        }
        if (toolsMigrationParametersTable.containsKey(Offset.GTID)) {
            sourceMap.put(Offset.GTID, toolsMigrationParametersTable.get(Offset.GTID));
        }
        sinkMap.put("xlog.location", toolsConfigParametersTable.get(Status.XLOG_PATH));
    }

    /**
     * init
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        if (checkAnotherConnectExists()) {
            LOGGER.error("{}Another connector is running.Cannot run incremental migration whose workspace id is {}.",
                    ErrorCode.MIGRATION_CONDITIONS_NOT_MET, workspaceId);
            return false;
        }
        try {
            findOffset();
        } catch (PortalException e) {
            LOGGER.error("{}{}", ErrorCode.LOAD_CONFIGURATION_ERROR, e.toString());
            PortalControl.shutDownPortal(e.toString());
            return false;
        }
        LOGGER.info("incremental migration tool start init");
        initConfigChangeParamsMap();
        setAllParams(workspaceId);
        changeAllConfig();
        deleteParamsConifg();
        Task.startTaskMethod(Method.Name.CONNECT_SOURCE, 5000, "", incrementalLogFileListener);
        return true;
    }

    /**
     *
     * initParmasFromEnvForAddAndChange
     */
    @Override
    public void initParmasFromEnvForAddAndChange() {
        sourceMap.putAll(ParamsUtils.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_MYSQL_SOURCE));
        sinkMap.putAll(ParamsUtils.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_MYSQL_SINK));
    }

    /**
     *
     * initParmasFromEnvForDelete
     */
    @Override
    public void initParmasFromEnvForDelete() {
        String mysqlSourceParams = System.getProperty(ToolsConfigEnum.DEBEZIUM_MYSQL_SOURCE.getConfigName());
        if (Strings.isNotBlank(mysqlSourceParams)) {
            configDeleteParamsMap.put(Debezium.Source.INCREMENTAL_CONFIG_PATH, List.of(mysqlSourceParams.split(",")));
        }
        String mysqlSinkParams = System.getProperty(ToolsConfigEnum.DEBEZIUM_MYSQL_SINK.getConfigName());
        if (Strings.isNotBlank(mysqlSinkParams)) {
            configDeleteParamsMap.put(Debezium.Sink.INCREMENTAL_CONFIG_PATH, List.of(mysqlSinkParams.split(",")));
        }
    }

    /**
     *
     * initConfigChangeParamsMap
     */
    @Override
    public void initConfigChangeParamsMap() {
        sourceMap = new HashMap<>();
        sinkMap = new HashMap<>();
        sourceConnectMap = new HashMap<>();
        sinkConnectMap = new HashMap<>();
        logMap = new HashMap<>();
        this.configPropsChangeParamsMap.put(Debezium.Source.INCREMENTAL_CONFIG_PATH, sourceMap);
        this.configPropsChangeParamsMap.put(Debezium.Sink.INCREMENTAL_CONFIG_PATH, sinkMap);
        this.configPropsChangeParamsMap.put(Debezium.Source.CONNECTOR_PATH, sourceConnectMap);
        this.configPropsChangeParamsMap.put(Debezium.Sink.CONNECTOR_PATH, sinkConnectMap);
        this.configPropsChangeParamsMap.put(LOG_PATTERN_PATH, logMap);
    }

    /**
     *
     * start
     */
    @Override
    public boolean start(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_INCREMENTAL_MIGRATION;
        }
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        String standaloneSinkFilePath = hashtable.get(Debezium.Sink.CONNECTOR_PATH);
        KafkaUtils.changekafkaLogParam(workspaceId + "_sink", hashtable.get(LOG_PATTERN_PATH));
        int sinkPort = StartPort.REST_MYSQL_SINK + PortalControl.portId * 10;
        int port = ParamsUtils.getAvailablePorts(sinkPort, 1, 1000).get(0);
        PropertitesUtils.changeSinglePropertiesParameter("rest.port", String.valueOf(port), standaloneSinkFilePath);
        Task.startTaskMethod(Method.Name.CONNECT_SINK, 5000, "", incrementalLogFileListener);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_INCREMENTAL_MIGRATION;
        }
        stop();
        return true;
    }

    /**
     *
     * stop
     */
    @Override
    public boolean stop() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration
                && !PortalControl.taskList.contains(Command.Start.Mysql.INCREMENTAL_CHECK)) {
            LOGGER.info("Incremental migration is running...");
            ProcessUtils.sleepThread(1000, "running incremental migraiton");
        }
        LOGGER.info("Plan.stopIncrementalMigration = {} Plan.stopPlan={}  PortalControl.taskList.contains(Command"
                        + ".Start.Mysql.INCREMENTAL_CHECK)={}", Plan.stopIncrementalMigration,
                Plan.stopPlan, PortalControl.taskList.contains(Command.Start.Mysql.INCREMENTAL_CHECK));
        List<String> taskThreadList = List.of(Method.Run.CONNECT_SINK, Method.Run.CONNECT_SOURCE);
        if (Plan.stopIncrementalMigration) {
            beforeStop(taskThreadList);
        }
        return true;
    }

    /**
     * Check another connect exists boolean.
     *
     * @return the boolean
     */
    public boolean checkAnotherConnectExists() {
        ArrayList<String> connectorParameterList = new ArrayList<>();
        connectorParameterList.add(Method.Run.REVERSE_CONNECT_SOURCE);
        connectorParameterList.add(Method.Run.CONNECT_SOURCE);
        connectorParameterList.add(Method.Run.CONNECT_SINK);
        connectorParameterList.add(Method.Run.REVERSE_CONNECT_SINK);
        for (String connectorParameter : connectorParameterList) {
            if (ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(connectorParameter)) != -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * uninstall
     * @return boolean
     */
    public boolean uninstall() {
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationUtils.removeSingleMigrationToolFiles(filePaths, errorPath);
        return true;
    }

    /**
     * checkStatus
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean checkStatus(String workspaceId) {
        checkProcess(Method.Run.CONNECT_SINK);
        checkProcess(Method.Run.CONNECT_SOURCE);
        return false;
    }

    /**
     * reportProgress
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean reportProgress(String workspaceId) {
        String sourceIncrementalStatusPath = "";
        String sinkIncrementalStatusPath = "";
        File directory = new File(toolsConfigParametersTable.get(Status.INCREMENTAL_FOLDER));
        if (directory.exists() && directory.isDirectory() && directory.listFiles() != null) {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                if (file.getName().contains("forward-source-process")) {
                    sourceIncrementalStatusPath = file.getAbsolutePath();
                } else if (file.getName().contains("forward-sink-process")) {
                    sinkIncrementalStatusPath = file.getAbsolutePath();
                }
            }
        }
        String incrementalStatusPath = toolsConfigParametersTable.get(Status.INCREMENTAL_PATH);
        if (new File(sourceIncrementalStatusPath).exists() && new File(sinkIncrementalStatusPath).exists()) {
            ChangeStatusTools.changeIncrementalStatus(sourceIncrementalStatusPath, sinkIncrementalStatusPath,
                    incrementalStatusPath, true);
        }
        return true;
    }

    /**
     * Before stop.
     *
     * @param taskThreadList the task thread list
     */
    public void beforeStop(List<String> taskThreadList) {
        LOGGER.info("beforeStop start...");
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.INCREMENTAL_MIGRATION_FINISHED;
            Plan.pause = true;
            ProcessUtils.sleepThread(50, "pausing the plan");
        }
        if (PortalControl.taskList.contains(Command.Start.Mysql.REVERSE)) {
            if (PortalControl.taskList.contains(Command.Start.Mysql.FULL)
                    && MysqlFullMigrationTool.shouldDetachReplica) {
                fullMigrationTool.runDetach();
            }
            try (PgConnection conn = JdbcUtils.getPgConnection()) {
                JdbcUtils.changeAllTable(conn);
                JdbcUtils.createLogicalReplicationSlot(conn);
            } catch (SQLException e) {
                PortalException portalException = new PortalException("SQL exception", "select global variable",
                        e.getMessage());
                portalException.setRequestInformation("Create slot failed.");
                ReverseMigrationTool.refuseReverseMigrationReason = portalException.getMessage();
                LOGGER.error("{}{}", ErrorCode.SQL_EXCEPTION, portalException.toString());
            }
        }
        for (String taskThread : taskThreadList) {
            Task.stopTaskMethod(taskThread);
        }
        PortalControl.status = Status.INCREMENTAL_MIGRATION_STOPPED;
        LOGGER.info("Incremental migration stopped.");
    }
}
