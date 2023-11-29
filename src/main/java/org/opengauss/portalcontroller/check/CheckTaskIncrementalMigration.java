/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.check;

import org.apache.logging.log4j.util.Strings;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.JdbcTools;
import org.opengauss.portalcontroller.Plan;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.Task;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.constant.ToolsConfigEnum;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.ConnectorMysql;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import static org.opengauss.portalcontroller.constant.Debezium.Connector.LOG_PATTERN_PATH;

/**
 * The type Check task incremental migration.
 */
public class CheckTaskIncrementalMigration implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskIncrementalMigration.class);
    private LogFileListener incrementalLogFileListener = new LogFileListener();

    public void installAllPackages(boolean download) throws PortalException {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new ConnectorMysql());
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        for (Software software : softwareArrayList) {
            installMigrationTools.installSingleMigrationSoftware(software, download);
        }
        Tools.outputResult(true, Command.Install.Mysql.IncrementalMigration.DEFAULT);
    }

    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Hashtable<String, String> hashtable1 = new Hashtable<>();
        hashtable1.put("name", "mysql-source-" + workspaceId);
        hashtable1.put("database.server.name", "mysql_server_" + workspaceId);
        hashtable1.put("database.server.id", String.valueOf(Tools.getCurrentPortalPid()));
        hashtable1.put("database.history.kafka.topic", "mysql_server_" + workspaceId + "_history");
        hashtable1.put("transforms.route.regex", "^" + "mysql_server_" + workspaceId + "(.*)");
        hashtable1.put("transforms.route.replacement", "mysql_server_" + workspaceId + "_topic");
        String incrementalFolder = hashtable.get(Status.INCREMENTAL_FOLDER);
        hashtable1.put("source.process.file.path", incrementalFolder);
        hashtable1.put("create.count.info.path", incrementalFolder);
        Tools.changePropertiesParameters(hashtable1, hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Hashtable<String, String> hashtable2 = new Hashtable<>();
        hashtable2.put("name", "mysql-sink-" + workspaceId);
        hashtable2.put("topics", "mysql_server_" + workspaceId + "_topic");
        hashtable2.put("record.breakpoint.kafka.topic", "mysql_bp_" + workspaceId + "_topic");
        try {
            Tools.createFile(incrementalFolder, false);
        } catch (PortalException e) {
            e.setRequestInformation("Create incremental migration folder status folder failed.Please ensure the config folder " + incrementalFolder + " is available");
            LOGGER.error(e.toString());
            return;
        }
        hashtable2.put("sink.process.file.path", incrementalFolder);
        hashtable2.put("create.count.info.path", incrementalFolder);
        hashtable2.put("xlog.location", hashtable.get(Status.XLOG_PATH));
        hashtable2.put("fail.sql.path", incrementalFolder);
        Tools.changePropertiesParameters(hashtable2, hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        Tools.changeConfluentParameters();
        if (Strings.isNotBlank(PortalControl.toolsMigrationParametersTable
                .get(MigrationParameters.Log.GLOBAL_LOG_LEVEL))) {
            Tools.changeSinglePropertiesParameter("log4j.rootLogger",
                    PortalControl.toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL)
                            .toUpperCase() + ", stdout, connectAppender",
                    PortalControl.toolsConfigParametersTable.get(LOG_PATTERN_PATH));
        }
    }

    @Override
    public void prepareWork(String workspaceId) {
        Tools.changeIncrementalMigrationParameters(PortalControl.toolsMigrationParametersTable);
        changeParameters(workspaceId);
        Tools.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_MYSQL_SOURCE,
                PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Tools.changeToolsPropsParameters(ToolsConfigEnum.DEBEZIUM_MYSQL_SINK,
                PortalControl.toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        Tools.deleteParams(ToolsConfigEnum.DEBEZIUM_MYSQL_SOURCE.getConfigName(),
                PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Tools.deleteParams(ToolsConfigEnum.DEBEZIUM_MYSQL_SINK.getConfigName(),
                PortalControl.toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        if (checkAnotherConnectExists()) {
            LOGGER.error("Another connector is running.Cannot run incremental migration whose workspace id is " + workspaceId + " .");
            return;
        }
        try {
            Tools.findOffset();
        } catch (PortalException e) {
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeConnectXmlFile(workspaceId + "_source", hashtable.get(LOG_PATTERN_PATH));
        String standaloneSourcePath = hashtable.get(Debezium.Source.CONNECTOR_PATH);
        int sourcePort = StartPort.REST_MYSQL_SOURCE + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sourcePort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), standaloneSourcePath);
        Task.startTaskMethod(Method.Name.CONNECT_SOURCE, 5000, "", incrementalLogFileListener);
    }

    @Override
    public void start(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_INCREMENTAL_MIGRATION;
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String standaloneSinkFilePath = hashtable.get(Debezium.Sink.CONNECTOR_PATH);
        Tools.changeConnectXmlFile(workspaceId + "_sink", hashtable.get(LOG_PATTERN_PATH));
        int sinkPort = StartPort.REST_MYSQL_SINK + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sinkPort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), standaloneSinkFilePath);
        Task.startTaskMethod(Method.Name.CONNECT_SINK, 5000, "", incrementalLogFileListener);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_INCREMENTAL_MIGRATION;
        }
        checkEnd();
    }

    @Override
    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration && !PortalControl.taskList.contains(Command.Start.Mysql.INCREMENTAL_CHECK)) {
            LOGGER.info("Incremental migration is running...");
            Tools.sleepThread(1000, "running incremental migraiton");
        }
        List<String> taskThreadList = List.of(Method.Run.CONNECT_SINK, Method.Run.CONNECT_SOURCE);
        if (Plan.stopIncrementalMigration) {
            beforeStop(taskThreadList);
        }
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
            if (Tools.getCommandPid(Task.getTaskProcessMap().get(connectorParameter)) != -1) {
                return true;
            }
        }
        return false;
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }

    /**
     * Before stop.
     *
     * @param taskThreadList the task thread list
     */
    public static void beforeStop(List<String> taskThreadList) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.INCREMENTAL_MIGRATION_FINISHED;
            Plan.pause = true;
            Tools.sleepThread(50, "pausing the plan");
        }
        if (PortalControl.taskList.contains(Command.Start.Mysql.REVERSE)) {
            if (PortalControl.taskList.contains(Command.Start.Mysql.FULL) && CheckTaskMysqlFullMigration.shouldDetachReplica) {
                CheckTaskMysqlFullMigration.runDetach();
            }
            try (PgConnection conn = JdbcTools.getPgConnection()) {
                JdbcTools.changeAllTable(conn);
                JdbcTools.createLogicalReplicationSlot(conn);
            } catch (SQLException e) {
                PortalException portalException = new PortalException("SQL exception", "select global variable", e.getMessage());
                portalException.setRequestInformation("Create slot failed.");
                PortalControl.refuseReverseMigrationReason = portalException.getMessage();
                LOGGER.error(portalException.toString());
            }
        }
        for (String taskThread : taskThreadList) {
            Task.stopTaskMethod(taskThread);
        }
        PortalControl.status = Status.INCREMENTAL_MIGRATION_STOPPED;
        LOGGER.info("Incremental migration stopped.");
    }
}
