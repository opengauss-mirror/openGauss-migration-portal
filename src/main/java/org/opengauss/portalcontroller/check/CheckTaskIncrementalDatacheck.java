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
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.Datacheck;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import static org.opengauss.portalcontroller.Plan.runningTaskList;

/**
 * The type Check task incremental datacheck.
 */
public class CheckTaskIncrementalDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskIncrementalDatacheck.class);
    private String workspaceId = "";

    /**
     * Gets workspace id.
     *
     * @return the workspace id
     */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /**
     * Sets workspace id.
     *
     * @param workspaceId the workspace id
     */
    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public void installAllPackages(boolean download) throws PortalException {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new Datacheck());
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        for (Software software : softwareArrayList) {
            installMigrationTools.installSingleMigrationSoftware(software, download);
        }
        Tools.outputResult(true, Command.Install.Mysql.Check.DEFAULT);
    }

    @Override
    public void prepareWork(String workspaceId) {
        runningTaskList.add(Command.Start.Mysql.INCREMENTAL_CHECK);
        changeParameters(workspaceId);
    }

    /**
     * Change datacheck parameters.
     */
    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSinglePropertiesParameter("offset.storage.file.filename", PathUtils.combainPath(true, PortalControl.portalControlPath + "tmp", "connect.offsets"), hashtable.get(Debezium.Connector.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.INCREMENTAL), hashtable.get(Check.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Sink.CONFIG_PATH));
        String sourceTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sourceTopic, hashtable.get(Check.Source.CONFIG_PATH));
        String sinkTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sinkTopic, hashtable.get(Check.Sink.CONFIG_PATH));
    }

    @Override
    public void start(String workspaceId) {
        Task.startTaskMethod(Method.Name.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Name.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Name.CHECK, 15000, "Started CheckApplication in");
        checkEnd();
    }

    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration) {
            LOGGER.info("Incremental migration is running...");
            Tools.outputDatacheckStatus(Parameter.CHECK_INCREMENTAL);
            Tools.sleepThread(1000, "running incremental migraiton datacheck");
        }
        List<String> taskThreadList = List.of(Method.Run.CHECK, Method.Run.CHECK_SINK, Method.Run.CHECK_SOURCE, Method.Run.CONNECT_SINK, Method.Run.CONNECT_SOURCE);
        if (Plan.stopIncrementalMigration) {
            CheckTaskIncrementalMigration.beforeStop(taskThreadList);
            LOGGER.info("Incremental migration datacheck stopped.");
        }
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
