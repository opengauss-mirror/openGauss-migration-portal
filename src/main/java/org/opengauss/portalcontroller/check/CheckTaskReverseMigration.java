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

import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.software.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Check task reverse migration.
 */
public class CheckTaskReverseMigration implements CheckTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskReverseMigration.class);

    private LogFileListener reverseLogFileListener = new LogFileListener();

    @Override
    public void installAllPackages(boolean download) throws PortalException {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new ConnectorOpengauss());
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        for (Software software : softwareArrayList) {
            installMigrationTools.installSingleMigrationSoftware(software, download);
        }
        Tools.outputResult(true, Command.Install.Mysql.ReverseMigration.DEFAULT);
    }

    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeReverseMigrationParameters(PortalControl.toolsMigrationParametersTable);
        String sourceConfigPath = hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH);
        String sinkConfigPath = hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        Hashtable<String, String> hashtable1 = new Hashtable<>();
        hashtable1.put("database.server.name", "opengauss_server_" + workspaceId);
        hashtable1.put("database.history.kafka.topic", "opengauss_server_" + workspaceId + "_history");
        hashtable1.put("transforms.route.regex", "^" + "opengauss_server_" + workspaceId + "(.*)");
        hashtable1.put("transforms.route.replacement", "opengauss_server_" + workspaceId + "_topic");
        hashtable1.put("source.process.file.path", hashtable.get(Status.REVERSE_FOLDER));
        hashtable1.put("create.count.info.path", hashtable.get(Status.REVERSE_FOLDER));
        hashtable1.put("slot.name", Plan.slotName);
        Tools.changePropertiesParameters(hashtable1, sourceConfigPath);
        Hashtable<String, String> hashtable2 = new Hashtable<>();
        hashtable2.put("topics", "opengauss_server_" + workspaceId + "_topic");
        hashtable2.put("record.breakpoint.kafka.topic", "opengauss_bp_" + workspaceId + "_topic");
        hashtable2.put("sink.process.file.path", hashtable.get(Status.REVERSE_FOLDER));
        hashtable2.put("create.count.info.path", hashtable.get(Status.REVERSE_FOLDER));
        hashtable2.put("fail.sql.path", hashtable.get(Status.REVERSE_FOLDER));
        Tools.changePropertiesParameters(hashtable2, sinkConfigPath);
        Tools.setXLogPath();
    }

    @Override
    public void prepareWork(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_REVERSE_MIGRATION;
        }
        changeParameters(workspaceId);
        if (!PortalControl.allowReverseMigration) {
            LOGGER.error("Can not run reverse migration" + PortalControl.refuseReverseMigrationReason);
            Plan.stopPlan = true;
            PortalControl.status = Status.ERROR;
            PortalControl.errorMsg = PortalControl.refuseReverseMigrationReason;
        }
    }

    @Override
    public void start(String workspaceId) {
        if (checkAnotherConnectExists()) {
            LOGGER.error("Another connector is running.Cannot run reverse migration with workspaceId is " + workspaceId + " .");
            return;
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        int sourcePort = StartPort.REST_OPENGAUSS_SOURCE + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sourcePort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH));
        Tools.changeConnectXmlFile(workspaceId + "_reverse_source", hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        Task.startTaskMethod(Method.Name.REVERSE_CONNECT_SOURCE, 8000, "", reverseLogFileListener);
        int sinkPort = StartPort.REST_OPENGAUSS_SINK + PortalControl.portId * 10;
        int port2 = Tools.getAvailablePorts(sinkPort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port2), hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH));
        Tools.changeConnectXmlFile(workspaceId + "_reverse_sink", hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        Task.startTaskMethod(Method.Name.REVERSE_CONNECT_SINK, 8000, "", reverseLogFileListener);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_REVERSE_MIGRATION;
        }
        checkEnd();
    }

    /**
     * Check another connect exists boolean.
     *
     * @return the boolean
     */
    public boolean checkAnotherConnectExists() {
        boolean flag1 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SOURCE)) != -1;
        boolean flag2 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SINK)) != -1;
        boolean flag3 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SOURCE)) != -1;
        boolean flag4 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SINK)) != -1;
        return flag1 || flag2 || flag3 || flag4;
    }

    @Override
    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration && !PortalControl.taskList.contains("start mysql reverse migration datacheck")) {
            LOGGER.info("Reverse migration is running...");
            Tools.sleepThread(1000, "running reverse migraiton");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                Tools.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
        }
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
}
