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

package org.opengauss.portalcontroller.status;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.thread.ThreadExceptionHandler;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.tools.mysql.FullDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.MysqlFullMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Thread status controller.
 */
public class ThreadStatusController extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadStatusController.class);

    /**
     * capacity reduced flag
     */
    private static boolean isReduced = false;

    private String workspaceId;

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

    /**
     * The Exit.
     */
    public boolean exit = false;

    /**
     * The constant fullMigrationStatus.
     */
    public static FullMigrationStatus fullMigrationStatus = new FullMigrationStatus();

    private static final Tool mysqlFullMigrationTool = new MysqlFullMigrationTool();
    private static final Tool fullDatacheckTool = new FullDatacheckTool();
    private static final Tool incrementalMigrationTool = new IncrementalMigrationTool();
    private static final Tool reverseMigrationTool = new ReverseMigrationTool();

    /**
     * The constant portalStatusWriterArrayList.
     */
    public static ArrayList<PortalStatusWriter> portalStatusWriterArrayList = new ArrayList<>();

    static {
        PortalStatusWriter psw = new PortalStatusWriter(Status.START_FULL_MIGRATION, System.currentTimeMillis());
        portalStatusWriterArrayList.add(psw);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        while (!exit) {
            ChangeStatusTools.reduceDiskSpace();

            fullMigrationAndDatacheckProgressReport();
            if (PortalControl.status < Status.START_REVERSE_MIGRATION && PortalControl.status > Status.FULL_MIGRATION_CHECK_FINISHED) {
                incrementalMigrationTool.reportProgress(workspaceId);
            }
            if (PortalControl.status >= Status.START_REVERSE_MIGRATION && PortalControl.status != Status.ERROR) {
                reverseMigrationTool.reportProgress(workspaceId);
            }
            ChangeStatusTools.writePortalStatus();

            try {
                String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
                Hashtable<String, String> toolsConfigHashtable = PortalControl.toolsConfigParametersTable;
                Hashtable<String, String> hashtable = new Hashtable<>();
                hashtable.put(PathUtils.combainPath(true, confluentPath + "logs", "server.log"),
                        toolsConfigHashtable.get(Debezium.LOG_PATH) + "server.log");
                hashtable.put(PathUtils.combainPath(true, confluentPath + "logs", "schema-registry.log"),
                        toolsConfigHashtable.get(Debezium.LOG_PATH) + "schema-registry.log");
                for (String key : hashtable.keySet()) {
                    if (new File(key).exists()) {
                        RuntimeExecUtils.copyFile(key, hashtable.get(key), true);
                    }
                }
                File logFile = new File(confluentPath + "logs");
                if (logFile.exists() && logFile.isDirectory()) {
                    File[] logFileList = logFile.listFiles();
                    String debeziumLogPath = toolsConfigHashtable.get(Debezium.LOG_PATH);
                    if (logFileList != null) {
                        for (File file : logFileList) {
                            RuntimeExecUtils.copyFileStartWithWord(file, debeziumLogPath,
                                    "connect_" + workspaceId + "_source.log",
                                    "connect_source.log", true);
                            RuntimeExecUtils.copyFileStartWithWord(file, debeziumLogPath,
                                    "connect_" + workspaceId + "_sink.log",
                                    "connect_sink.log", true);
                            RuntimeExecUtils.copyFileStartWithWord(file, debeziumLogPath,
                                    "connect_" + workspaceId + "_reverse_source.log",
                                    "reverse_connect_source.log", true);
                            RuntimeExecUtils.copyFileStartWithWord(file, debeziumLogPath,
                                    "connect_" + workspaceId + "_reverse_sink.log",
                                    "reverse_connect_sink.log", true);
                        }
                    }
                }
            } catch (PortalException e) {
                e.setRequestInformation("Cannot find logs.");
                LOGGER.error(e.toString());
                PortalControl.shutDownPortal(e.toString());
            }
            ProcessUtils.sleepThread(2000, "writing the status");
        }
        isReduced = false;
    }

    /**
     * Reports the progress of full migration and full migration data check.
     */
    public void fullMigrationAndDatacheckProgressReport() {
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        String path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_init_replica.json";
        if (new File(path).exists()) {
            mysqlFullMigrationTool.reportProgress(workspaceId);
            fullDatacheckTool.reportProgress(workspaceId);
        }
    }

    public static boolean isReduced() {
        return isReduced;
    }

    public static void setIsReduced(boolean isReduced) {
        ThreadStatusController.isReduced = isReduced;
    }
}
