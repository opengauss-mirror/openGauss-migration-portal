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

package org.opengauss.portalcontroller.task;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.DataCheckLogFileCheck;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.status.CheckColumnRule;
import org.opengauss.portalcontroller.status.CheckRule;
import org.opengauss.portalcontroller.status.RuleParameter;
import org.opengauss.portalcontroller.thread.ThreadCheckProcess;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.tools.mysql.FullDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.MysqlFullMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;
import org.opengauss.portalcontroller.utils.YmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;

/**
 * Plan.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public final class Plan {
    public static final String START_MYSQL_FULL_MIGRATION = "start mysql full migration";
    public static final String START_MYSQL_FULL_MIGRATION_DATACHECK = "start mysql full migration datacheck";
    public static final String START_MYSQL_INCREMENTAL_MIGRATION = "start mysql incremental migration";
    public static final String START_MYSQL_INCREMENTAL_MIGRATION_DATACHECK = "start mysql incremental migration datacheck";
    public static final String START_MYSQL_REVERSE_MIGRATION = "start mysql reverse migration";
    public static final String START_MYSQL_REVERSE_MIGRATION_DATACHECK = "start mysql reverse migration datacheck";
    private static volatile Plan plan;

    private Plan() {

    }


    /**
     * The constant threadCheckProcess.
     */
    public static ThreadCheckProcess threadCheckProcess = new ThreadCheckProcess();

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
     * The index of table in schema.table.
     */
    public static Integer INDEX_TABLE = 1;

    /**
     * the indcex of schema in schema.table
     */
    private static Integer indexSchema = 0;

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
    public static final List<Tool> toolList = new ArrayList<>();

    /**
     * Wait for incremental signal.
     *
     * @param msg the msg
     */
    public static void waitForIncrementalSignal(String msg) {
        while (true) {
            ProcessUtils.sleepThread(1000, "waiting for signal");
            if (runReverseMigration || runIncrementalMigration || stopPlan) {
                LOGGER.info(msg);
                break;
            }
        }
    }

    /**
     * Wait for reverse signal.
     *
     * @param msg the msg
     */
    public static void waitForReverseSignal(String msg) {
        while (true) {
            ProcessUtils.sleepThread(1000, "waiting for signal");
            if (runReverseMigration || stopPlan) {
                LOGGER.info(msg);
                break;
            }
        }
    }

    /**
     * Change command line parameters.
     */
    public static void changeCommandLineParameters() {
        String checkSinkPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.CONFIG_PATH);
        String checkSourcePath = PortalControl.toolsConfigParametersTable.get(Check.Source.CONFIG_PATH);
        HashMap<String, Object> checkSinkOldTable = YmlUtils.getYmlParameters(checkSinkPath);
        HashMap<String, Object> checkSinkTable = new HashMap<>();
        checkSinkTable.put(Check.Parameters.QUERY_DOP, Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.QUERY_DOP,
                checkSinkOldTable.get(Check.Parameters.QUERY_DOP).toString())));
        checkSinkTable.put(Check.Parameters.INITIAL_SIZE,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.INITIAL_SIZE,
                        checkSinkOldTable.get(Check.Parameters.INITIAL_SIZE).toString())));
        checkSinkTable.put(Check.Parameters.MIN_IDLE, Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.MIN_IDLE,
                checkSinkOldTable.get(Check.Parameters.MIN_IDLE).toString())));
        checkSinkTable.put(Check.Parameters.MAX_ACTIVE,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.MAX_ACTIVE,
                        checkSinkOldTable.get(Check.Parameters.MAX_ACTIVE).toString())));
        checkSinkTable.put(Check.Parameters.TIME_PERIOD,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.TIME_PERIOD,
                        checkSinkOldTable.get(Check.Parameters.TIME_PERIOD).toString())));
        checkSinkTable.put(Check.Parameters.NUM_PERIOD,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Sink.NUM_PERIOD,
                        checkSinkOldTable.get(Check.Parameters.NUM_PERIOD).toString())));
        YmlUtils.changeYmlParameters(checkSinkTable, checkSinkPath);
        HashMap<String, Object> checkSourceOldTable = YmlUtils.getYmlParameters(checkSourcePath);
        HashMap<String, Object> checkSourceTable = new HashMap<>();
        checkSourceTable.put(Check.Parameters.QUERY_DOP,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.QUERY_DOP,
                        checkSourceOldTable.get(Check.Parameters.QUERY_DOP).toString())));
        checkSourceTable.put(Check.Parameters.INITIAL_SIZE,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.INITIAL_SIZE,
                        checkSourceOldTable.get(Check.Parameters.INITIAL_SIZE).toString())));
        checkSourceTable.put(Check.Parameters.MIN_IDLE,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.MIN_IDLE,
                        checkSourceOldTable.get(Check.Parameters.MIN_IDLE).toString())));
        checkSourceTable.put(Check.Parameters.MAX_ACTIVE,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.MAX_ACTIVE,
                        checkSourceOldTable.get(Check.Parameters.MAX_ACTIVE).toString())));
        checkSourceTable.put(Check.Parameters.TIME_PERIOD,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.TIME_PERIOD,
                        checkSourceOldTable.get(Check.Parameters.TIME_PERIOD).toString())));
        checkSourceTable.put(Check.Parameters.NUM_PERIOD,
                Integer.parseInt(ParamsUtils.getOrDefault(Check.Source.NUM_PERIOD,
                        checkSourceOldTable.get(Check.Parameters.NUM_PERIOD).toString())));
        YmlUtils.changeYmlParameters(checkSourceTable, checkSourcePath);
        writeCheckRules();
    }

    /**
     * Write check rules.
     */
    private static void writeCheckRules() {
        String path = PortalControl.toolsConfigParametersTable.get(Check.CONFIG_PATH);
        HashMap<String, Object> checkConfigHashMap = YmlUtils.getYmlParameters(path);
        RuleParameter tableRuleParameter = new RuleParameter(Check.Rules.Table.AMOUNT, Check.Rules.Table.NAME,
                Check.Rules.Table.TEXT, "");
        RuleParameter rowRuleParameter = new RuleParameter(Check.Rules.Row.AMOUNT, Check.Rules.Row.NAME,
                Check.Rules.Row.TEXT, "");
        RuleParameter columnRuleParameter = new RuleParameter(Check.Rules.Column.AMOUNT, Check.Rules.Column.NAME,
                Check.Rules.Column.TEXT, Check.Rules.Column.ATTRIBUTE);
        String rulesEnableParameter = ParamsUtils.getOrDefault(Check.Rules.ENABLE,
                String.valueOf(checkConfigHashMap.get(Check.Rules.ENABLE)));
        checkConfigHashMap.put(Check.Rules.ENABLE, Boolean.valueOf(rulesEnableParameter));
        getCheckRulesFromCommandLine(checkConfigHashMap, tableRuleParameter, false);
        addTableRuleParameter(checkConfigHashMap, tableRuleParameter,
                toolsMigrationParametersTable.get(Mysql.DATABASE_TABLE));
        getCheckRulesFromCommandLine(checkConfigHashMap, rowRuleParameter, false);
        getCheckRulesFromCommandLine(checkConfigHashMap, columnRuleParameter, true);
        YmlUtils.changeYmlParameters(checkConfigHashMap, path);
    }

    /**
     * Gets table white
     *
     * @param hashMap       check config
     * @param ruleParameter table rule parameter
     * @param tableWhite    table white list
     */
    private static void addTableRuleParameter(HashMap<String, Object> hashMap,
                                              RuleParameter ruleParameter, String tableWhite) {
        if (!Plan.isRuleEnable(tableWhite)) {
            return;
        }
        if (hashMap.containsKey(ruleParameter.getAmount())) {
            return;
        }
        String[] dbTables = tableWhite.split(",");
        ArrayList<Object> objectArrayList = new ArrayList<>();
        String schema = toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
        for (String dt : dbTables) {
            addCheckRule(schema, dt, objectArrayList);
        }
        if (!objectArrayList.isEmpty()) {
            hashMap.put(ruleParameter.getAmount(), objectArrayList);
        }
    }

    private static void addCheckRule(String schema, String dt, ArrayList<Object> objectArrayList) {
        CheckRule checkRule;
        String[] schemaTable = dt.trim().split("\\.");
        if (schemaTable.length != 2) {
            return;
        }
        if (schemaTable[indexSchema].equalsIgnoreCase(schema)) {
            checkRule = new CheckRule("white", schemaTable[INDEX_TABLE].trim());
            Object jsonObject = JSON.toJSON(checkRule);
            if (Objects.nonNull(jsonObject)) {
                objectArrayList.add(jsonObject);
            }
        }
    }

    /**
     * Gets rule enable
     *
     * @param dbTable
     * @return
     */
    public static boolean isRuleEnable(String dbTable) {
        if (dbTable == null || dbTable.isBlank() || dbTable.equals("null")) {
            return false;
        }
        return true;
    }

    /**
     * Gets check rules from command line.
     *
     * @param hashMap       the hash map
     * @param ruleParameter the rule parameter
     * @param hasAttribute  the has attribute
     */
    public static void getCheckRulesFromCommandLine(HashMap<String, Object> hashMap, RuleParameter ruleParameter,
                                                    boolean hasAttribute) {
        ArrayList<CheckRule> checkRules = new ArrayList<>();
        String ruleAmount = ruleParameter.getAmount();
        String ruleName = ruleParameter.getName();
        String ruleText = ruleParameter.getText();
        String ruleAttribute = ruleParameter.getAttribute();
        if (System.getProperty(ruleAmount) != null) {
            int amount = Integer.parseInt(System.getProperty(ruleAmount));
            for (int i = 1; i <= amount; i++) {
                CheckRule checkRule;
                String name = System.getProperty(ruleName + i);
                String text = System.getProperty(ruleText + i);
                if (hasAttribute) {
                    String attribute = System.getProperty(ruleAttribute + i);
                    checkRule = new CheckColumnRule(name, text, attribute);
                } else {
                    checkRule = new CheckRule(name, text);
                }
                checkRules.add(checkRule);
            }
            changeCheckRules(hashMap, ruleAmount, checkRules);
        }
    }

    /**
     * Change check rules hash map.
     *
     * @param oldMap     the old map
     * @param key        the key
     * @param checkRules the check rules
     */
    public static void changeCheckRules(HashMap<String, Object> oldMap, String key, ArrayList<CheckRule> checkRules) {
        ArrayList<Object> objectArrayList = new ArrayList<>();
        for (CheckRule checkRule : checkRules) {
            Object jsonObject = JSON.toJSON(checkRule);
            objectArrayList.add(jsonObject);
        }
        if (oldMap.containsKey(key)) {
            oldMap.replace(key, objectArrayList);
        } else {
            oldMap.put(key, objectArrayList);
        }
    }

    /**
     * Sets port id.
     *
     * @param name the name
     * @return the port id
     */
    public static int setPortId(String name) {
        int portId = -1;
        try {
            File pidFile = new File(name);
            RandomAccessFile randomAccessFile = new RandomAccessFile(pidFile, "rw");
            FileInputStream fileInputStream = new FileInputStream(pidFile);
            FileChannel channel = randomAccessFile.getChannel();
            FileLock lock = channel.tryLock();
            if (lock != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
                String idString = br.readLine();
                portId = idString == null ? 0 : Integer.parseInt(idString.trim());
                br.close();
                portId++;
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pidFile)));
                bw.write(String.valueOf(portId));
                bw.flush();
                bw.close();
                lock.release();
                lock.close();
            }
            channel.close();
            fileInputStream.close();
            randomAccessFile.close();
        } catch (IOException | NumberFormatException e) {
            LOGGER.error("Error massage: Get lock failed.", e);
        }
        return portId;
    }

    /**
     * Is full datacheck success boolean.
     *
     * @return the boolean
     */
    public static boolean isFullDatacheckSuccess() {
        boolean runningFullDatacheck = PortalControl.status >= Status.START_FULL_MIGRATION_CHECK;
        try {
            TimeUnit.SECONDS.sleep(6);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException:", e);
        }
        LOGGER.info("isFullDatacheckSuccess finish flag = {}", DataCheckLogFileCheck.isDataCheckFinish());
        return runningFullDatacheck && DataCheckLogFileCheck.isDataCheckFinish();
    }


    /**
     * Exec plan.
     *
     * @param taskList the task list
     */
    public void execPlan(List<String> taskList) {
        threadCheckProcess.setName("threadCheckProcess");
        threadCheckProcess.start();
        Task.initRunTaskHandlerHashMap();
        PortalControl.showMigrationParameters();
        if (isPlanRunnable) {
            adjustKernelParam();
            isPlanRunnable = false;
            MysqlFullMigrationTool mysqlFullMigrationTool = new MysqlFullMigrationTool();

            if (taskList.contains(START_MYSQL_FULL_MIGRATION)) {
                mysqlFullMigrationTool.init(workspaceId);
            }
            IncrementalMigrationTool incrementalMigrationTool = new IncrementalMigrationTool();
            if (taskList.contains(START_MYSQL_INCREMENTAL_MIGRATION)) {
                incrementalMigrationTool.init(workspaceId);
            }
            if (taskList.contains(START_MYSQL_FULL_MIGRATION)) {
                mysqlFullMigrationTool.start(workspaceId);
            }
            FullDatacheckTool fullDatacheckTool = new FullDatacheckTool();
            if (taskList.contains(START_MYSQL_FULL_MIGRATION_DATACHECK)) {
                fullDatacheckTool.init(workspaceId);
                fullDatacheckTool.start(workspaceId);
            }
            if (taskList.contains(START_MYSQL_INCREMENTAL_MIGRATION)) {
                IncrementalDatacheckTool incrementalDatacheckTool = new IncrementalDatacheckTool();
                while (true) {
                    incrementalMigrationTool.start(workspaceId);
                    if (taskList.contains(START_MYSQL_INCREMENTAL_MIGRATION_DATACHECK)) {
                        incrementalDatacheckTool.init(workspaceId);
                        incrementalDatacheckTool.start(workspaceId);
                    }
                    waitForIncrementalSignal("Incremental migration has stopped.");
                    if (runReverseMigration || stopPlan) {
                        Plan.pause = false;
                        break;
                    }
                    if (runIncrementalMigration) {
                        incrementalMigrationTool.init(workspaceId);
                    }
                }
            }
            if (taskList.contains(START_MYSQL_REVERSE_MIGRATION) && !stopPlan) {
                ReverseMigrationTool reverseMigrationTool = new ReverseMigrationTool();
                ReverseDatacheckTool reverseDatacheckTool = new ReverseDatacheckTool();
                while (true) {
                    reverseMigrationTool.init(workspaceId);
                    reverseMigrationTool.start(workspaceId);
                    if (taskList.contains(START_MYSQL_REVERSE_MIGRATION_DATACHECK)) {
                        reverseDatacheckTool.init(workspaceId);
                        reverseDatacheckTool.start(workspaceId);
                    }
                    waitForReverseSignal("Reverse migration has stopped.");
                    if (stopPlan) {
                        Plan.pause = false;
                        break;
                    }
                }
            }
            PortalControl.threadStatusController.fullMigrationAndDatacheckProgressReport();
            ChangeStatusTools.writePortalStatus();
            Plan.stopPlan = true;
            Plan.stopPlanThreads();
            if (PortalControl.status == Status.ERROR) {
                LOGGER.error("Plan failed.");
            } else {
                LOGGER.info("Plan finished.");
            }
            restoreKernelParam();
            threadCheckProcess.exit = true;
        } else {
            LOGGER.error("There is a plan running.Please stop current plan or wait.");
        }
    }

    private void adjustKernelParam() {
        String isAdjustKernelParam = System.getProperty("is_adjustKernel_param", "false");
        LOGGER.info("is_adjustKernel_param is {}", isAdjustKernelParam);
        if (!Boolean.parseBoolean(isAdjustKernelParam)) {
            LOGGER.info("no adjust kernel parameter.");
            return;
        }
        LOGGER.info("adjust kernel parameter start.");
        String databaseAdjustParamsPath = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config",
                "databaseAdjustParams.properties");
        Hashtable<String, String> databaseKernelParams =
                PropertitesUtils.getPropertiesParameters(databaseAdjustParamsPath);
        LOGGER.info("databaseKernelParams is {}", databaseKernelParams);
        String databaseOldParamsPath = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config",
                "databaseOldParams.properties");
        PgConnection pgConnection = null;
        try {
            pgConnection = JdbcUtils.getPgConnection();
            ParamsUtils.writeMapToProperties(JdbcUtils.queryParam(pgConnection, databaseKernelParams),
                    databaseOldParamsPath);
            JdbcUtils.adjustDatabaseParam(pgConnection, databaseKernelParams);
        } finally {
            JdbcUtils.closeConnection(pgConnection);
        }
        LOGGER.info("adjust kernel parameter end.");
    }

    private void restoreKernelParam() {
        String isAdjustKernelParam = System.getProperty("is_adjustKernel_param", "false");
        LOGGER.info("is_adjustKernel_param is {}", isAdjustKernelParam);
        if (!Boolean.parseBoolean(isAdjustKernelParam)) {
            LOGGER.info("no restore kernel parameter.");
            return;
        }
        LOGGER.info("restore kernel parameter start.");
        String databaseOldParamsPath = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config",
                "databaseOldParams.properties");
        Hashtable<String, String> databaseKernelParams =
                PropertitesUtils.getPropertiesParameters(databaseOldParamsPath);
        LOGGER.info("databaseOldParams is {}", databaseKernelParams);
        FileUtils.deleteQuietly(new File(databaseOldParamsPath));
        PgConnection pgConnection = null;
        try {
            pgConnection = JdbcUtils.getPgConnection();
            JdbcUtils.adjustDatabaseParam(pgConnection, databaseKernelParams);
        } finally {
            JdbcUtils.closeConnection(pgConnection);
        }
        LOGGER.info("restore kernel parameter end.");
    }

    /**
     * Stop plan threads.
     */
    public static void stopPlanThreads() {
        LOGGER.info("Stop plan.");
        ProcessUtils.closeAllProcess("--config default_" + workspaceId + " --");
        threadCheckProcess.exit = true;
        stopAllTasks();
        Plan.clean();
        Plan.runningTaskThreadsList.clear();
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
        boolean isAlive = isKafkaAlive();
        if (!isAlive) {
            PortalControl.status = Status.ERROR;
            PortalControl.errorMsg = "During the task, the processes of Kafka or it's components are interrupted.";
            LOGGER.error(PortalControl.errorMsg);
            Plan.stopPlan = true;
            return false;
        }
        if (!runningTaskThreadsList.isEmpty()) {
            boolean cleanFullDataCheck = false;
            for (RunningTaskThread thread : runningTaskThreadsList) {
                int pid = ProcessUtils.getCommandPid(thread.getProcessName());
                if (pid == -1) {
                    if (thread.getMethodName().contains("Check") && Plan.isFullDatacheckRunning
                            && isFullDatacheckSuccess()) {
                        cleanFullDataCheck = true;
                        break;
                    } else if (Plan.pause) {
                        LOGGER.warn("Plan paused.Stop checking threads.");
                        break;
                    } else {
                        Task.getCheckProcessMap().get(thread.getName()).checkStatus();
                        Plan.stopPlan = true;
                        isAlive = false;
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
        return isAlive;
    }

    /**
     * Check kafka and it's components' processes
     *
     * @return true if the kafka, zookeeper and schema registry is running
     */
    public static boolean isKafkaAlive() {
        ArrayList<String> stringArrayList = new ArrayList<>();
        stringArrayList.add(Method.Run.ZOOKEEPER);
        stringArrayList.add(Method.Run.KAFKA);
        stringArrayList.add(Method.Run.REGISTRY);
        for (String methodName : stringArrayList) {
            if (ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(methodName)) == -1) {
                LOGGER.error("Start methond={} failed.", methodName);
                return false;
            }
        }
        return true;
    }

    /**
     * Create workspace boolean.
     *
     * @param workspaceId the workspace id
     */
    public static void createWorkspace(String workspaceId) {
        try {
            KafkaUtils.changeConfluentDirFromSysParam();
            WorkspacePath workspacePath = WorkspacePath.getInstance(PortalControl.portalControlPath, workspaceId);
            String portIdFile = PortalControl.portalControlPath + "portal.portId.lock";
            org.opengauss.portalcontroller.utils.FileUtils.createFile(portIdFile, true);
            PortalControl.portId = setPortId(portIdFile) % 100;
            String path = workspacePath.getWorkspacePath();
            org.opengauss.portalcontroller.utils.FileUtils.createFile(workspacePath.getWorkspacePath(), false);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(PathUtils.combainPath(false, path, "tmp"), false);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(workspacePath.getWorkspaceLogPath(), false);
            RuntimeExecUtils.copyFileIfNotExist(PathUtils.combainPath(false, PortalControl.portalControlPath
                    + "config"), path);
            PortalControl.initHashTable();
            Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.FOLDER), false);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.INCREMENTAL_FOLDER), false);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.PORTAL_PATH), true);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.FULL_PATH), true);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.INCREMENTAL_PATH), true);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Status.REVERSE_PATH), true);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Debezium.LOG_PATH), false);
            org.opengauss.portalcontroller.utils.FileUtils.createFile(hashtable.get(Check.LOG_FOLDER), false);
            String connectorStandaloneConfigPath = hashtable.get(Debezium.Connector.CONFIG_PATH);
            Hashtable<String, String> table2 = new Hashtable<>();
            table2.put("offset.storage.file.filename", PathUtils.combainPath(true,
                    PortalControl.portalWorkSpacePath + "tmp", "connect.offsets"));
            table2.put("plugin.path", "share/java, " + hashtable.get(Debezium.Connector.PATH));
            PropertitesUtils.changePropertiesParameters(table2, hashtable.get(Debezium.Connector.CONFIG_PATH));
            RuntimeExecUtils.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Source.CONNECTOR_PATH),
                    false);
            RuntimeExecUtils.copyFile(connectorStandaloneConfigPath, hashtable.get(Debezium.Sink.CONNECTOR_PATH),
                    false);
            RuntimeExecUtils.copyFile(connectorStandaloneConfigPath,
                    hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH), false);
            RuntimeExecUtils.copyFile(connectorStandaloneConfigPath,
                    hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH), false);
            ParamsUtils.changeDatacheckLogPath(Check.LOG_PATTERN_PATH);
            ParamsUtils.changeDatacheckLogPath(Check.Source.LOG_PATTERN_PATH);
            ParamsUtils.changeDatacheckLogPath(Check.Sink.LOG_PATTERN_PATH);
            ParamsUtils.changeDatacheckLogLevel(Check.LOG_PATTERN_PATH);
            changeCommandLineParameters();
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
            MysqlFullMigrationTool checkTaskMysqlFullMigration = new MysqlFullMigrationTool();
            checkTaskMysqlFullMigration.cleanData(workspaceId);
        }
        if (PortalControl.taskList.contains(Command.Start.Mysql.REVERSE)
                && PortalControl.toolsMigrationParametersTable.get(Check.DROP_LOGICAL_SLOT).equals("true")) {
            try (PgConnection conn = JdbcUtils.getPgConnection()) {
                JdbcUtils.changeAllTable(conn);
                JdbcUtils.dropLogicalReplicationSlot(conn);
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
                PortalException portalException = new PortalException("Interrupted exception", "stopping the plan",
                        e.getMessage());
                LOGGER.error(portalException.toString());
                return;
            }
            for (String runName : runArrayList) {
                if (ProcessUtils.getRunningTaskPid(runName) != -1) {
                    flag = true;
                    break;
                }
            }
        }
    }
}


