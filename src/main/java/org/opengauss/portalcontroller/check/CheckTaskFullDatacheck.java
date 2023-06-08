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
import org.opengauss.portalcontroller.constant.*;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.Datacheck;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Check task full datacheck.
 */
public class CheckTaskFullDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskFullDatacheck.class);
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

    @Override
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
        Plan.runningTaskList.add(Command.Start.Mysql.FULL_CHECK);
        changeParameters(workspaceId);
    }

    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", false, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", false, hashtable.get(Check.Sink.CONFIG_PATH));
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.FULL), hashtable.get(Check.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
    }

    @Override
    public void start(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION_CHECK;
        }
        Task.startTaskMethod(Method.Name.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Name.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Name.CHECK, 15000, "Started CheckApplication in");
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_FULL_MIGRATION_CHECK;
        }
        checkEnd();
    }

    public void checkEnd() {
        while (!Plan.stopPlan) {
            if (Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CHECK)) == -1) {
                if (PortalControl.status != Status.ERROR) {
                    LOGGER.info("Full migration datacheck is finished.");
                    PortalControl.status = Status.FULL_MIGRATION_CHECK_FINISHED;
                }
                break;
            }
            Tools.outputDatacheckStatus(Parameter.CHECK_FULL);
            Tools.sleepThread(LogParseConstants.PERIOD_WATCH_LOG, "running full migration datacheck");
        }
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
