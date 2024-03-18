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
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.constant.ToolsConfigEnum;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.ConnectorOpengauss;
import org.opengauss.portalcontroller.software.Software;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;
import static org.opengauss.portalcontroller.utils.ProcessUtils.checkProcess;

/**
 * ReverseMigrationTool
 *
 * @date :2023/11/3 15:22
 * @description: ReverseMigrationTool
 * @version: 1.1
 * @since 1.1
 */
public class ReverseMigrationTool extends ParamsConfig implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReverseMigrationTool.class);
    public static final String XLOG_LOCATION = "xlog.location";
    /**
     * The constant allowReverseMigration.
     */
    public static boolean allowReverseMigration = false;
    /**
     * The constant refuseReverseMigrationReason.
     */
    public static String refuseReverseMigrationReason = "";

    private final LogFileListener reverseLogFileListener = new LogFileListener();

    private Map<String, Object> reverseSourceParams = null;
    private Map<String, Object> reverseSinkParams = null;

    private Map<String, Object> reverseConnectSinkParams = null;
    private Map<String, Object> reverseConnectSourceParams = null;

    /**
     * Check reverse migration runnable boolean.
     *
     * @return the boolean
     */
    public static boolean checkReverseMigrationRunnable() {
        boolean isReverseRunnable = false;
        try (PgConnection connection = JdbcUtils.getPgConnection()) {
            Hashtable<String, String> parameterTable = new Hashtable<>();
            parameterTable.put("wal_level", "logical");
            int parameter = 0;
            for (String key : parameterTable.keySet()) {
                if (JdbcUtils.selectGlobalVariables(connection, key, parameterTable.get(key))) {
                    parameter++;
                } else {
                    break;
                }
            }
            if (parameter == parameterTable.size()) {
                isReverseRunnable = true;
            }
        } catch (SQLException e) {
            PortalException portalException = new PortalException("IO exception",
                    "checking reverse migration is runnable", e.getMessage());
            refuseReverseMigrationReason = portalException.getMessage();
            LOGGER.error(portalException.toString());
        }
        allowReverseMigration = isReverseRunnable;
        return isReverseRunnable;
    }

    /**
     * Change reverse migration parameters.
     */
    @Override
    public void initDataBaseParams() {
        reverseSourceParams.put(Debezium.Source.HOST, toolsMigrationParametersTable.get(Opengauss.DATABASE_HOST));
        reverseSourceParams.put(Debezium.Source.PORT, toolsMigrationParametersTable.get(Opengauss.DATABASE_PORT));
        reverseSourceParams.put(Debezium.Source.USER, toolsMigrationParametersTable.get(Opengauss.USER));
        reverseSourceParams.put(Debezium.Source.PASSWORD, toolsMigrationParametersTable.get(Opengauss.PASSWORD));
        reverseSourceParams.put(Debezium.Source.NAME, toolsMigrationParametersTable.get(Opengauss.DATABASE_NAME));
        reverseSinkParams.put(Debezium.Sink.Mysql.USER, toolsMigrationParametersTable.get(Mysql.USER));
        reverseSinkParams.put(Debezium.Sink.Mysql.PASSWORD, toolsMigrationParametersTable.get(Mysql.PASSWORD));
        reverseSinkParams.put(Debezium.Sink.Mysql.NAME, toolsMigrationParametersTable.get(Mysql.DATABASE_NAME));
        reverseSinkParams.put(Debezium.Sink.Mysql.PORT, toolsMigrationParametersTable.get(Mysql.DATABASE_PORT));
        reverseSinkParams.put(Debezium.Sink.Mysql.URL, toolsMigrationParametersTable.get(Mysql.DATABASE_HOST));
        String mysqlDatabaseName = toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
        String openGaussSchema = toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA);
        reverseSinkParams.put(Debezium.Sink.SCHEMA_MAPPING, openGaussSchema + ":" + mysqlDatabaseName);
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
        softwareArrayList.add(new ConnectorOpengauss());
        InstallMigrationUtils installMigrationUtils = new InstallMigrationUtils();
        for (Software software : softwareArrayList) {
            try {
                installMigrationUtils.installSingleMigrationSoftware(software, isDownload);
            } catch (PortalException e) {
                LOGGER.error("install failed", e);
                return false;
            }
        }
        LogViewUtils.outputResult(true, Command.Install.Mysql.ReverseMigration.DEFAULT);
        return true;
    }

    /**
     * initWorkSpaceParams
     *
     * @param workspaceId workspaceId
     */
    @Override
    public void initWorkSpaceParams(String workspaceId) {
        reverseSourceParams.put("database.server.name", "opengauss_server_" + workspaceId);
        reverseSourceParams.put("database.history.kafka.topic", "opengauss_server_" + workspaceId + "_history");
        reverseSourceParams.put("transforms.route.regex", "^" + "opengauss_server_" + workspaceId + "(.*)");
        reverseSourceParams.put("transforms.route.replacement", "opengauss_server_" + workspaceId + "_topic");
        reverseSourceParams.put("source.process.file.path", toolsConfigParametersTable.get(Status.REVERSE_FOLDER));
        reverseSourceParams.put("create.count.info.path", toolsConfigParametersTable.get(Status.REVERSE_FOLDER));
        if (Plan.slotName == null || "".equals(Plan.slotName.trim())) {
            Plan.slotName = "slot_" + workspaceId;
        }
        reverseSourceParams.put("slot.name", Plan.slotName);
        reverseSinkParams.put("topics", "opengauss_server_" + workspaceId + "_topic");
        reverseSinkParams.put("record.breakpoint.kafka.topic", "opengauss_bp_" + workspaceId + "_topic");
        reverseSinkParams.put("sink.process.file.path", toolsConfigParametersTable.get(Status.REVERSE_FOLDER));
        reverseSinkParams.put("create.count.info.path", toolsConfigParametersTable.get(Status.REVERSE_FOLDER));
        reverseSinkParams.put("fail.sql.path", toolsConfigParametersTable.get(Status.REVERSE_FOLDER));

    }

    /**
     *
     * initInteractionParams
     */
    @Override
    void initInteractionParams() {
        String xLogPath = PortalControl.toolsConfigParametersTable.get(Status.XLOG_PATH);
        String xLogLocation = "";
        File file = new File(xLogPath);
        try {
            if (file.exists()) {
                BufferedReader fileReader = new BufferedReader((new InputStreamReader(new FileInputStream(file))));
                String tempStr;
                while ((tempStr = fileReader.readLine()) != null) {
                    if (tempStr.contains(XLOG_LOCATION)) {
                        int index = tempStr.lastIndexOf("=") + 1;
                        xLogLocation = tempStr.substring(index).trim();
                    }
                }
                fileReader.close();
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "reading xlog.path in file " + file.getAbsolutePath(), e.getMessage());
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
            return;
        }
        reverseSourceParams.put(XLOG_LOCATION, xLogLocation);
    }

    /**
     *
     * initKafkaParams
     */
    @Override
    public void initKafkaParams() {
        // connect-avro-standalone.properties 文件修改
        MigrationConfluentInstanceConfig portalConfig = MigrationConfluentInstanceConfig.getInstanceFromPortalConfig();
        HashMap<String, Object> connectAvroStandalonePropChangeParam = new HashMap<>();
        String schemaRegistryPrefix = "http://";
        connectAvroStandalonePropChangeParam.put("bootstrap.servers", portalConfig.getKafkaIpPort());
        connectAvroStandalonePropChangeParam.put("key.converter.schema.registry.url",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        connectAvroStandalonePropChangeParam.put("value.converter.schema.registry.url",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        reverseConnectSinkParams.putAll(connectAvroStandalonePropChangeParam);
        reverseConnectSourceParams.putAll(connectAvroStandalonePropChangeParam);
        // opengauss-sink.properties文件修改
        reverseSinkParams.put("record.breakpoint.kafka.bootstrap.servers", portalConfig.getKafkaIpPort());
    }

    /**
     * init
     *
     * @param workspaceId  workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        if (!allowReverseMigration) {
            LOGGER.error("Can not run reverse migration" + refuseReverseMigrationReason);
            Plan.stopPlan = true;
            PortalControl.status = Status.ERROR;
            PortalControl.errorMsg = refuseReverseMigrationReason;
            return false;
        }
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_REVERSE_MIGRATION;
        }
        LOGGER.info("reverse migration tool start init");
        initConfigChangeParamsMap();
        setAllParams(workspaceId);
        changeAllConfig();
        deleteParamsConifg();
        return true;
    }

    /**
     *
     * initParmasFromEnvForDelete
     */
    @Override
    public void initParmasFromEnvForDelete() {
        String openGaussSinkParams = System.getProperty(ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SINK.getConfigName());
        if (Strings.isNotBlank(openGaussSinkParams)) {
            configDeleteParamsMap.put(Debezium.Sink.REVERSE_CONFIG_PATH, List.of(openGaussSinkParams.split(",")));
        }
        String openGaussSourceParams = System.getProperty(ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SOURCE.getConfigName());
        if (Strings.isNotBlank(openGaussSourceParams)) {
            configDeleteParamsMap.put(Debezium.Source.REVERSE_CONFIG_PATH, List.of(openGaussSourceParams.split(",")));
        }
    }

    /**
     *
     * initParmasFromEnvForAddAndChange
     */
    @Override
    public void initParmasFromEnvForAddAndChange() {
        reverseSinkParams.putAll(ParamsUtils.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SINK));
        reverseSourceParams.putAll(ParamsUtils.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SOURCE));
    }

    /**
     *
     * initConfigChangeParamsMap
     */
    @Override
    public void initConfigChangeParamsMap() {
        reverseSourceParams = new HashMap<>();
        reverseSinkParams = new HashMap<>();
        reverseConnectSinkParams = new HashMap<>();
        reverseConnectSourceParams = new HashMap<>();
        this.configPropsChangeParamsMap.put(Debezium.Source.REVERSE_CONFIG_PATH, reverseSourceParams);
        this.configPropsChangeParamsMap.put(Debezium.Sink.REVERSE_CONFIG_PATH, reverseSinkParams);
        this.configPropsChangeParamsMap.put(Debezium.Source.REVERSE_CONNECTOR_PATH, reverseConnectSourceParams);
        this.configPropsChangeParamsMap.put(Debezium.Sink.REVERSE_CONNECTOR_PATH, reverseConnectSinkParams);
    }

    /**
     * start
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean start(String workspaceId) {
        if (checkAnotherConnectExists()) {
            LOGGER.error("Another connector is running.Cannot run reverse migration with workspaceId is "
                    + workspaceId + " .");
            return false;
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        int sourcePort = StartPort.REST_OPENGAUSS_SOURCE + PortalControl.portId * 10;
        int port = ParamsUtils.getAvailablePorts(sourcePort, 1, 1000).get(0);
        PropertitesUtils.changeSinglePropertiesParameter("rest.port", String.valueOf(port),
                hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH));
        KafkaUtils.changekafkaLogParam(workspaceId + "_reverse_source",
                hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        Task.startTaskMethod(Method.Name.REVERSE_CONNECT_SOURCE, 8000, "", reverseLogFileListener);
        int sinkPort = StartPort.REST_OPENGAUSS_SINK + PortalControl.portId * 10;
        int port2 = ParamsUtils.getAvailablePorts(sinkPort, 1, 1000).get(0);
        PropertitesUtils.changeSinglePropertiesParameter("rest.port", String.valueOf(port2),
                hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH));
        KafkaUtils.changekafkaLogParam(workspaceId + "_reverse_sink",
                hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        Task.startTaskMethod(Method.Name.REVERSE_CONNECT_SINK, 8000, "", reverseLogFileListener);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_REVERSE_MIGRATION;
        }
        stop();
        return true;
    }

    /**
     * Check another connect exists boolean.
     *
     * @return the boolean
     */
    public boolean checkAnotherConnectExists() {
        boolean hasSouce =
                ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SOURCE)) != -1;
        boolean hasSink =
                ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SINK)) != -1;
        boolean hasConnectSource =
                ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SOURCE)) != -1;
        boolean hasConnectSink =
                ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SINK)) != -1;
        return hasSouce || hasSink || hasConnectSource || hasConnectSink;
    }

    /**
     * stop
     *
     * @return boolean
     */
    @Override
    public boolean stop() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration && !PortalControl.taskList.contains("start mysql reverse "
                + "migration datacheck")) {
            LOGGER.info("Reverse migration is running...");
            ProcessUtils.sleepThread(1000, "running reverse migraiton");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                ProcessUtils.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
        }
        return true;
    }

    /**
     * uninstall
     *
     * @return boolean
     */
    @Override
    public boolean uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
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
     * @param workspaceId  workspaceId
     * @return boolean
     */
    @Override
    public boolean checkStatus(String workspaceId) {
        checkProcess(Method.Run.REVERSE_CONNECT_SINK);
        checkProcess(Method.Run.REVERSE_CONNECT_SOURCE);
        return true;
    }

    /**
     * reportProgress
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean reportProgress(String workspaceId) {
        String sourceReverseStatusPath = "";
        String sinkReverseStatusPath = "";
        File directory = new File(toolsConfigParametersTable.get(Status.REVERSE_FOLDER));
        if (directory.exists() && directory.isDirectory() && directory.listFiles() != null) {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                if (file.getName().contains("reverse-source-process")) {
                    sourceReverseStatusPath = file.getAbsolutePath();
                } else if (file.getName().contains("reverse-sink-process")) {
                    sinkReverseStatusPath = file.getAbsolutePath();
                }
            }
        }
        String reverseStatusPath = toolsConfigParametersTable.get(Status.REVERSE_PATH);
        if (new File(sourceReverseStatusPath).exists() && new File(sinkReverseStatusPath).exists()) {
            ChangeStatusTools.changeIncrementalStatus(sourceReverseStatusPath, sinkReverseStatusPath,
                    reverseStatusPath, false);
        }
        return true;
    }
}
