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
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.Plan.runningTaskList;

/**
 * The type Check task reverse datacheck.
 */
public class CheckTaskReverseDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskReverseDatacheck.class);

    @Override
    public void installAllPackages(boolean download) throws PortalException {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new Datacheck());
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        for (Software software : softwareArrayList) {
            installMigrationTools.installSingleMigrationSoftware(software, download);
        }
        Tools.outputResult(true, Command.Install.Mysql.Check.DEFAULT);
    }

    /**
     * Change datacheck parameters.
     */
    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSinglePropertiesParameter("dataDir", hashtable.get(Debezium.Zookeeper.TMP_PATH), hashtable.get(Debezium.Zookeeper.CONFIG_PATH));
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        kafkaConfigTable.put("log.dirs", hashtable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        Tools.changePropertiesParameters(kafkaConfigTable, hashtable.get(Debezium.Kafka.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.REVERSE), hashtable.get(Check.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Sink.CONFIG_PATH));
        String sourceTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sourceTopic, hashtable.get(Check.Source.CONFIG_PATH));
        String sinkTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sinkTopic, hashtable.get(Check.Sink.CONFIG_PATH));
    }

    @Override
    public void prepareWork(String workspaceId) {
        runningTaskList.add(Command.Start.Mysql.REVERSE_CHECK);
        Task.startTaskMethod(Method.Run.ZOOKEEPER, 8000, "");
        Task.startTaskMethod(Method.Run.KAFKA, 8000, "");
        Task.startTaskMethod(Method.Run.REGISTRY, 5000, "");
        changeParameters(workspaceId);
    }

    @Override
    public void start(String workspaceId) {
        Task.startTaskMethod(Method.Run.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK, 15000, "Started CheckApplication in");
        checkEnd();
    }

    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration) {
            LOGGER.info("Reverse migration is running...");
            if (!Tools.outputDatacheckStatus(Parameter.CHECK_REVERSE)) {
                break;
            }
            Tools.sleepThread(1000, "running reverse migraiton datacheck");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                Tools.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.CHECK);
            Task.stopTaskMethod(Method.Run.CHECK_SINK);
            Task.stopTaskMethod(Method.Run.CHECK_SOURCE);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
        }
    }

    public void uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.PATH));
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
