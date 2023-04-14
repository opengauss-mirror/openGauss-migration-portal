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
import org.opengauss.portalcontroller.constant.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
     * The constant workspacePath.
     */
    public static String workspacePath = "";


    /**
     * Sets workspace id.
     *
     * @param workspaceId the workspace id
     */
    public static void setWorkspaceId(String workspaceId) {
        Plan.workspaceId = workspaceId;
    }

    /**
     * Get currentTask.
     *
     * @return String currentTask.
     */
    public static String getCurrentTask() {
        return currentTask;
    }

    /**
     * Set currentTask.
     *
     * @param currentTask currentTask.
     */
    public static void setCurrentTask(String currentTask) {
        Plan.currentTask = currentTask;
    }

    /**
     * Hashmap to save string and the lambda expression.
     */
    public static HashMap<String, PortalControl.EventHandler> taskHandlerHashMap = new HashMap<>();

    /**
     * Get running task list.
     */
    public static List<String> runningTaskList = new ArrayList<>();

    /**
     * Boolean parameter express that the plan is runnable.
     */
    public static boolean isPlanRunnable = true;

    /**
     * Boolean parameter express that the plan is stopping.
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
     * Get a instance of class plan.
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
     * Get running threads list.
     *
     * @return runningTaskThreadsList running task threads list
     */
    public static List<RunningTaskThread> getRunningTaskThreadsList() {
        return runningTaskThreadsList;
    }

    /**
     * Set running threads list.
     *
     * @param runningThreadList runningThreadList
     */
    public static void setRunningTaskThreadsList(List<RunningTaskThread> runningThreadList) {
        Plan.runningTaskThreadsList = runningThreadList;
    }

    /**
     * The constant checkTaskList.
     */
    public static List<CheckTask> checkTaskList = new ArrayList<>();


    /**
     * Execute plan.
     *
     * @param taskList The task list of the plan.
     */
    public void execPlan(List<String> taskList) {
        Task.initRunTaskHandlerHashMap();
        Task.initStopTaskHandlerHashMap();
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
     * Stop Plan.
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
     * Check running threads whose pid of process changed.
     *
     * @return flag A boolean parameter express that threads are running.
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
                        LOGGER.warn("Plan paused.Stop checking threads. ");
                        break;
                    } else {
                        String[] str = thread.getProcessName().split(" ");
                        String logPath = thread.getLogPath();
                        String errorStr = "Error message: Process " + str[0] + " exit abnormally or process " + str[0]
                                + " has started. " + System.lineSeparator();
                        errorStr += Tools.getErrorMsg(logPath) + System.lineSeparator();
                        errorStr += "Please read " + logPath + " or error.log to get information. ";
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
                    if (runningTaskThreadsList.get(i).getMethodName().contains("Check")) {
                        runningTaskThreadsList.remove(i);
                    }
                }
                Plan.isFullDatacheckRunning = false;
            }
        }
        return flag;
    }

    /**
     * Create workspace boolean.
     *
     * @param workspaceId the workspace id
     * @return the boolean
     */
    public static boolean createWorkspace(String workspaceId) {
        String portIdFile = PortalControl.portalControlPath + "portal.portId.lock";
        Tools.createFile(portIdFile, true);
        PortalControl.portId = Tools.setPortId(portIdFile) % 100;
        boolean flag = true;
        String path = PortalControl.portalControlPath + "workspace" + File.separator + workspaceId + File.separator;
        Tools.createFile(path, false);
        Tools.createFile(path + "tmp", false);
        Tools.createFile(path + "logs", false);
        workspacePath = path;
        RuntimeExecTools.copyFile(PortalControl.portalControlPath + "config" + File.separator, path, false);
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
        table2.put("offset.storage.file.filename", PortalControl.portalWorkSpacePath + "tmp" + File.separator + "connect.offsets");
        table2.put("plugin.path", "share/java, " + hashtable.get(Debezium.Connector.PATH));
        Tools.changePropertiesParameters(table2, hashtable.get(Debezium.Connector.CONFIG_PATH));
        RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Source.CONNECTOR_PATH), false);
        RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Sink.CONNECTOR_PATH), false);
        RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH), false);
        RuntimeExecTools.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH), false);
        Tools.changeFile("/tmp/datacheck/logs", PortalControl.portalWorkSpacePath + File.separator + "logs" + File.separator + "datacheck", hashtable.get(Check.LOG_PATTERN_PATH));
        Tools.changeFile("/tmp/datacheck/logs", PortalControl.portalWorkSpacePath + File.separator + "logs" + File.separator + "datacheck", hashtable.get(Check.Source.LOG_PATTERN_PATH));
        Tools.changeFile("/tmp/datacheck/logs", PortalControl.portalWorkSpacePath + File.separator + "logs" + File.separator + "datacheck", hashtable.get(Check.Sink.LOG_PATTERN_PATH));
        Tools.changeCommandLineParameters();
        return flag;
    }

    /**
     * Install plan packages.
     */
    public static void installPlanPackages() {
        for (CheckTask checkTask : Plan.checkTaskList) {
            checkTask.installAllPackages();
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
            try {
                PgConnection conn = JdbcTools.getPgConnection();
                JdbcTools.changeAllTable(conn);
                String slotName = Plan.slotName;
                JdbcTools.dropLogicalReplicationSlot(conn, slotName);
                conn.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * Stop all tasks.
     */
    public static void stopAllTasks() {
        Task task = new Task();
        task.stopDataCheck();
        Tools.sleepThread(100, "stopping the plan");
        task.stopDataCheckSink();
        Tools.sleepThread(100, "stopping the plan");
        task.stopDataCheckSource();
        Tools.sleepThread(100, "stopping the plan");
        task.stopReverseKafkaConnectSink();
        Tools.sleepThread(100, "stopping the plan");
        task.stopReverseKafkaConnectSource();
        Tools.sleepThread(100, "stopping the plan");
        task.stopKafkaConnectSink();
        Tools.sleepThread(100, "stopping the plan");
        task.stopKafkaConnectSource();
        Tools.sleepThread(100, "stopping the plan");
        task.stopKafkaSchema(PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH));
        Tools.sleepThread(1000, "stopping the plan");
        task.stopKafka(PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH));
        Tools.sleepThread(1000, "stopping the plan");
        task.stopZookeeper(PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH));
        Tools.sleepThread(1000, "stopping the plan");
    }
}


