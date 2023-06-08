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

package org.opengauss.portalcontroller;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.check.*;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Plan.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public final class Plan {
    private static volatile Plan plan;

    private Plan() {

    }

    private static volatile List<RunningTaskThread> runningTaskThreadsList = new CopyOnWriteArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Plan.class);
    private static String currentTask = "";

    /**
     * The constant workspaceId.
     */
    public static String workspaceId = "";

    /**
     * Sets workspace id.
     *
     * @param workspaceId the workspace id
     */
    public static void setWorkspaceId(String workspaceId) {
        Plan.workspaceId = workspaceId;
    }

    /**
     * Gets current task.
     *
     * @return the current task
     */
    public static String getCurrentTask() {
        return currentTask;
    }

    /**
     * Sets current task.
     *
     * @param currentTask the current task
     */
    public static void setCurrentTask(String currentTask) {
        Plan.currentTask = currentTask;
    }

    /**
     * The constant runningTaskList.
     */
    public static List<String> runningTaskList = new ArrayList<>();

    /**
     * The constant isPlanRunnable.
     */
    public static boolean isPlanRunnable = true;

    /**
     * The constant stopPlan.
     */
    public static boolean stopPlan = false;

    /**
     * The constant isFullDatacheckRunning.
     */
    public static boolean isFullDatacheckRunning = true;

    /**
     * The constant stopIncrementalMigration.
     */
    public static boolean stopIncrementalMigration = false;
    /**
     * The constant stopReverseMigration.
     */
    public static boolean stopReverseMigration = false;
    /**
     * The constant runReverseMigration.
     */
    public static boolean runReverseMigration = false;
    /**
     * The constant runIncrementalMigration.
     */
    public static boolean runIncrementalMigration = false;

    /**
     * The constant pause.
     */
    public static boolean pause = false;

    /**
     * The constant slotName.
     */
    public static String slotName = "";

    /**
     * Gets instance.
     *
     * @param workspaceID the workspace id
     * @return the instance
     */
    public static Plan getInstance(String workspaceID) {
        if (plan == null) {
            synchronized (Plan.class) {
                if (plan == null) {
                    plan = new Plan();
                    Plan.setWorkspaceId(workspaceID);
                }
            }
        }
        return plan;
    }

    /**
     * Check full datacheck running.
     */
    public static void checkFullDatacheckRunning() {
        isFullDatacheckRunning = false;
        LOGGER.info("Full datacheck is finished.");
    }

    /**
     * Gets running task threads list.
     *
     * @return the running task threads list
     */
    public static List<RunningTaskThread> getRunningTaskThreadsList() {
        return runningTaskThreadsList;
    }

    /**
     * Sets running task threads list.
     *
     * @param runningThreadList the running thread list
     */
    public static void setRunningTaskThreadsList(List<RunningTaskThread> runningThreadList) {
        Plan.runningTaskThreadsList = runningThreadList;
    }

    /**
     * The constant checkTaskList.
     */
    public static List<CheckTask> checkTaskList = new ArrayList<>();


    /**
     * Exec plan.
     *
     * @param taskList the task list
     */
    public void execPlan(List<String> taskList) {
        Task.initRunTaskHandlerHashMap();
        PortalControl.showMigrationParameters();
        if (isPlanRunnable) {
            isPlanRunnable = false;
            CheckTaskMysqlFullMigration checkTaskMysqlFullMigration = new CheckTaskMysqlFullMigration();
            CheckTaskFullDatacheck checkTaskFullDatacheck = new CheckTaskFullDatacheck();
            CheckTaskIncrementalMigration checkTaskIncrementalMigration = new CheckTaskIncrementalMigration();
            CheckTaskReverseMigration checkTaskReverseMigration = new CheckTaskReverseMigration();
            CheckTaskIncrementalDatacheck checkTaskIncrementalDatacheck = new CheckTaskIncrementalDatacheck();
            CheckTaskReverseDatacheck checkTaskReverseDatacheck = new CheckTaskReverseDatacheck();
            if (taskList.contains("start mysql full migration")) {
                checkTaskMysqlFullMigration.prepareWork(workspaceId);
            }
            if (taskList.contains("start mysql incremental migration")) {
                checkTaskIncrementalMigration.prepareWork(workspaceId);
            }
            if (taskList.contains("start mysql full migration")) {
                checkTaskMysqlFullMigration.start(workspaceId);
            }
            if (taskList.contains("start mysql full migration datacheck")) {
                checkTaskFullDatacheck.prepareWork(workspaceId);
                checkTaskFullDatacheck.start(workspaceId);
            }
            if (taskList.contains("start mysql incremental migration")) {
                while (true) {
                    checkTaskIncrementalMigration.start(workspaceId);
                    if (taskList.contains("start mysql incremental migration datacheck")) {
                        checkTaskIncrementalDatacheck.prepareWork(workspaceId);
                        checkTaskIncrementalDatacheck.start(workspaceId);
                    }
                    Tools.waitForIncrementalSignal("Incremental migration has stopped.");
                    if (runReverseMigration || stopPlan) {
                        Plan.pause = false;
                        break;
                    }
                    if (runIncrementalMigration) {
                        checkTaskIncrementalMigration.prepareWork(workspaceId);
                    }
                }
            }
            if (taskList.contains("start mysql reverse migration") && !stopPlan) {
                while (true) {
                    checkTaskReverseMigration.prepareWork(workspaceId);
                    checkTaskReverseMigration.start(workspaceId);
                    if (taskList.contains("start mysql reverse migration datacheck")) {
                        checkTaskReverseDatacheck.prepareWork(workspaceId);
                        checkTaskReverseDatacheck.start(workspaceId);
                    }
                    Tools.waitForReverseSignal("Reverse migration has stopped.");
                    if (stopPlan) {
                        Plan.pause = false;
                        break;
                    }
                }
            }
            Plan.stopPlan = true;
            Plan.stopPlanThreads();
            if (PortalControl.status == Status.ERROR) {
                LOGGER.error("Plan failed.");
            } else {
                LOGGER.info("Plan finished.");
            }
            PortalControl.threadCheckProcess.exit = true;
        } else {
            LOGGER.error("There is a plan running.Please stop current plan or wait.");
        }
    }

    /**
     * Stop plan threads.
     */
    public static void stopPlanThreads() {
        LOGGER.info("Stop plan.");
        Tools.closeAllProcess("--config default_" + workspaceId + " --");
        PortalControl.threadCheckProcess.exit = true;
        stopAllTasks();
        Plan.clean();
        Plan.runningTaskThreadsList.clear();
        Plan.runningTaskList.clear();
        Plan.currentTask = "";
        PortalControl.taskList.clear();
        isPlanRunnable = true;
    }

    /**
     * Check running threads boolean.
     *
     * @return the boolean
     */
    public static boolean checkRunningThreads() {
        boolean flag = true;
        if (runningTaskThreadsList.size() != 0) {
            boolean cleanFullDataCheck = false;
            for (RunningTaskThread thread : runningTaskThreadsList) {
                int pid = Tools.getCommandPid(thread.getProcessName());
                if (pid == -1) {
                    if (thread.getMethodName().contains("Check") && Plan.isFullDatacheckRunning
                            && Tools.isFullDatacheckSuccess()) {
                        cleanFullDataCheck = true;
                        break;
                    } else if (Plan.pause) {
                        LOGGER.warn("Plan paused.Stop checking threads.");
                        break;
                    } else {
                        String[] str = thread.getProcessName().split(" ");
                        String logPath = thread.getLogPath();
                        String errorStr = "Error message: Process " + str[0] + " exit abnormally or process " + str[0]
                                + " has started." + System.lineSeparator();
                        errorStr += LogView.getErrorMsg(logPath) + System.lineSeparator();
                        errorStr += "Please read " + logPath + " or error.log to get information.";
                        PortalControl.status = Status.ERROR;
                        PortalControl.errorMsg = errorStr;
                        LOGGER.error(errorStr);
                        Plan.stopPlan = true;
                        flag = false;
                    }
                }
            }
            if (cleanFullDataCheck) {
                int length = runningTaskThreadsList.size();
                for (int i = length - 1; i >= 0; i--) {
                    String methodName = runningTaskThreadsList.get(i).getMethodName();
                    LOGGER.info("Running task thread {} is in list.", methodName);
                    if (methodName.contains("Check")) {
                        runningTaskThreadsList.remove(i);
                        LOGGER.info("Remove task thread {} in list.", methodName);
                    }
                }
                Plan.checkFullDatacheckRunning();
            }
        }
        return flag;
    }

    /**
     * Create workspace boolean.
     *
     * @param workspaceId the workspace id
     */
    public static void createWorkspace(String workspaceId) {
        try {
            WorkspacePath workspacePath = WorkspacePath.getInstance(PortalControl.portalControlPath, workspaceId);
            String portIdFile = PortalControl.portalControlPath + "portal.portId.lock";
            Tools.createFile(portIdFile, true);
            PortalControl.portId = Tools.setPortId(portIdFile) % 100;
            String path = workspacePath.getWorkspacePath();
            Tools.createFile(workspacePath.getWorkspacePath(), false);
            Tools.createFile(PathUtils.combainPath(false, path, "tmp"), false);
            Tools.createFile(workspacePath.getWorkspaceLogPath(), false);
            RuntimeExecTools.copyFile(PathUtils.combainPath(false, PortalControl.portalControlPath + "config"), path, false);
            PortalControl.initHashTable();
            Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
            Tools.createFile(hashtable.get(Status.FOLDER), false);
            Tools.createFile(hashtable.get(Status.INCREMENTAL_FOLDER), false);
            Tools.createFile(hashtable.get(Status.PORTAL_PATH), true);
            Tools.createFile(hashtable.get(Status.FULL_PATH), true);
            Tools.createFile(hashtable.get(Status.INCREMENTAL_PATH), true);
            Tools.createFile(hashtable.get(Status.REVERSE_PATH), true);
            Tools.createFile(hashtable.get(Debezium.LOG_PATH), false);
            Tools.createFile(hashtable.get(Check.LOG_FOLDER), false);
            String connectorStandaloneConfigPath = hashtable.get(Debezium.Connector.CONFIG_PATH);
            Hashtable<String, String> table2 = new Hashtable<>();
            table2.put("offset.storage.file.filename", PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "tmp", "connect.offsets"));
            table2.put("plugin.path", "share/java, " + hashtable.get(Debezium.Connector.PATH));
            Tools.changePropertiesParameters(table2, hashtable.get(Debezium.Connector.CONFIG_PATH));
            RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Source.CONNECTOR_PATH), false);
            RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Sink.CONNECTOR_PATH), false);
            RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH), false);
            RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH), false);
            String dataCheckLogPath = PathUtils.combainPath(true, workspacePath.getWorkspaceLogPath(), "datacheck");
            Tools.changeFile("/tmp/datacheck/logs", dataCheckLogPath, hashtable.get(Check.LOG_PATTERN_PATH));
            Tools.changeFile("/tmp/datacheck/logs", dataCheckLogPath, hashtable.get(Check.Source.LOG_PATTERN_PATH));
            Tools.changeFile("/tmp/datacheck/logs", dataCheckLogPath, hashtable.get(Check.Sink.LOG_PATTERN_PATH));
            Tools.changeCommandLineParameters();
        } catch (PortalException e) {
            e.setRequestInformation("Create workspace failed");
            LOGGER.error(e.toString());
            Plan.stopPlan = true;
        }
    }

    /**
     * Clean.
     */
    public static void clean() {
        if (PortalControl.taskList.contains(Command.Start.Mysql.FULL)) {
            CheckTaskMysqlFullMigration checkTaskMysqlFullMigration = new CheckTaskMysqlFullMigration();
            checkTaskMysqlFullMigration.cleanData(workspaceId);
        }
        if (PortalControl.taskList.contains(Command.Start.Mysql.REVERSE)) {
            try (PgConnection conn = JdbcTools.getPgConnection()) {
                JdbcTools.changeAllTable(conn);
                JdbcTools.dropLogicalReplicationSlot(conn);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * Stop all tasks.
     */
    public static void stopAllTasks() {
        ArrayList<String> runArrayList = new ArrayList<>();
        runArrayList.add(Method.Run.CHECK);
        runArrayList.add(Method.Run.CHECK_SOURCE);
        runArrayList.add(Method.Run.CHECK_SINK);
        runArrayList.add(Method.Run.REVERSE_CONNECT_SOURCE);
        runArrayList.add(Method.Run.REVERSE_CONNECT_SINK);
        runArrayList.add(Method.Run.CONNECT_SOURCE);
        runArrayList.add(Method.Run.CONNECT_SINK);
        boolean flag = true;
        for (String runName : runArrayList) {
            Task.stopTaskMethod(runName);
        }
        while (flag) {
            flag = false;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                PortalException portalException = new PortalException("Interrupted exception", "stopping the plan", e.getMessage());
                LOGGER.error(portalException.toString());
                return;
            }
            for (String runName : runArrayList) {
                if (Tools.getRunningTaskPid(runName) != -1) {
                    flag = true;
                    break;
                }
            }
        }
    }
}


