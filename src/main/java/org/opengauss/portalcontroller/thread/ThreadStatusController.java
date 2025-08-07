/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.thread;

import lombok.Getter;
import lombok.Setter;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.handler.ThreadExceptionHandler;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.status.FullMigrationStatus;
import org.opengauss.portalcontroller.status.PortalStatusWriter;
import org.opengauss.portalcontroller.task.Plan;
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
import java.util.Hashtable;
import java.util.LinkedList;

/**
 * The type Thread status controller.
 *
 * @since 2024/12/24
 */
public class ThreadStatusController extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadStatusController.class);
    private static final Tool mysqlFullMigrationTool = new MysqlFullMigrationTool();
    private static final Tool fullDatacheckTool = new FullDatacheckTool();
    private static final Tool incrementalMigrationTool = new IncrementalMigrationTool();
    private static final Tool reverseMigrationTool = new ReverseMigrationTool();

    /**
     * capacity reduced flag
     */
    private static boolean isReduced = false;

    @Getter
    @Setter
    private static FullMigrationStatus fullMigrationStatus = new FullMigrationStatus();

    @Getter
    private static LinkedList<PortalStatusWriter> portalStatusWriterList = new LinkedList<>();

    static {
        PortalStatusWriter psw = new PortalStatusWriter(Status.START_FULL_MIGRATION,
                ChangeStatusTools.getCurrentTimestamp());
        portalStatusWriterList.add(psw);
    }

    @Setter
    @Getter
    private String workspaceId;

    @Setter
    private boolean isExit = false;

    /**
     * if the last status is equal to the given status
     *
     * @param status status
     * @return boolean
     */
    public synchronized static boolean isEqualLastPortalStatus(int status) {
        return portalStatusWriterList.getLast().getStatus() == status;
    }

    /**
     * add portal status writer list
     *
     * @param psw psw
     */
    public static void addPortalStatusWriterList(PortalStatusWriter psw) {
        portalStatusWriterList.add(psw);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        while (!isExit) {
            ChangeStatusTools.reduceDiskSpace();

            fullMigrationAndDatacheckProgressReport();
            if (PortalControl.status < Status.START_REVERSE_MIGRATION
                    && PortalControl.status > Status.FULL_MIGRATION_CHECK_FINISHED) {
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
                LOGGER.error("{}Failed to copy kafka logs to task workspace", ErrorCode.FILE_NOT_FOUND, e);
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
