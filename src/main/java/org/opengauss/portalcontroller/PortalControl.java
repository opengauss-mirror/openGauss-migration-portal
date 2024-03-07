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

import org.opengauss.portalcontroller.command.ConcreteCommand;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Offset;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Regex;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.status.ThreadStatusController;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.task.WorkspacePath;
import org.opengauss.portalcontroller.thread.ThreadExceptionHandler;
import org.opengauss.portalcontroller.thread.ThreadGetOrder;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.opengauss.portalcontroller.utils.ParamsUtils.initMigrationParamsFromProps;

/**
 * Portal control.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class PortalControl {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PortalControl.class);

    /**
     * The constant taskList.
     */
    public static List<String> taskList = new ArrayList<>();

    /**
     * The constant planList.
     */
    public static Hashtable<String, List<String>> planList = new Hashtable<>();

    /**
     * The constant validOrderList.
     */
    public static List<String> validOrderList = new ArrayList<>();

    /**
     * The constant portalControlPath.
     */
    public static String portalControlPath = "";

    /**
     * The constant portalErrorPath.
     */
    public static String portalErrorPath = "";

    /**
     * The constant portalWorkSpacePath.
     */
    public static String portalWorkSpacePath = "";

    /**
     * The constant toolsConfigPath.
     */
    public static String toolsConfigPath = "";

    /**
     * The constant migrationConfigPath.
     */
    public static String migrationConfigPath = "";

    /**
     * The constant toolsConfigParametersTable.
     */
    public static Hashtable<String, String> toolsConfigParametersTable = new Hashtable<>();

    /**
     * The constant parametersRegexMap.
     */
    public static HashMap<String, String> parametersRegexMap = new HashMap<>();

    /**
     * The constant toolsMigrationParametersTable.
     */
    public static Hashtable<String, String> toolsMigrationParametersTable = new Hashtable<>();


    /**
     * The constant commandLineParameterStringMap.
     */
    public static HashMap<String, String> commandLineParameterStringMap = new HashMap<>();

    /**
     * The constant latestCommand.
     */
    public static String latestCommand = "";

    /**
     * The constant status.
     */
    public static int status = Status.START_FULL_MIGRATION;

    /**
     * The constant portId.
     */
    public static int portId = 1;

    /**
     * The constant threadGetOrder.
     */
    public static ThreadGetOrder threadGetOrder = new ThreadGetOrder();
    /**
     * The constant threadStatusController.
     */
    public static ThreadStatusController threadStatusController = new ThreadStatusController();

    /**
     * The constant errorMsg.
     */
    public static String errorMsg = "";

    /**
     * The constant workspaceId.
     */
    public static String workspaceId = "1";

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        init();
        FileUtils.cleanInputOrder();
        threadGetOrder.start();
        String order = commandLineParameterStringMap.get(Command.Parameters.ORDER);
        String standardOrder = order.replaceAll("_", " ").trim();
        if (validOrderList.contains(standardOrder)) {
            ConcreteCommand concreteCommand = new ConcreteCommand();
            concreteCommand.execute(standardOrder);
        } else {
            LOGGER.error("Invalid command.");
        }
        threadGetOrder.exit = true;
    }

    private static void init() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        initPlanList();
        initParametersRegexMap();
        initCommandLineParameters();
        initValidOrderList();
        initPortalPath();
        initMigrationParamsFromProps();
        Plan.createWorkspace(workspaceId);
        Task.initMethodNameMap();
        Task.initTaskProcessMap();
        Task.initTaskLogMap();
        Task.initCheckProcessMap();
    }

    /**
     * Init tasklist.
     *
     * @param path the path
     * @return the array list
     */
    public static ArrayList<String> initTasklist(String path) {
        ArrayList<String> taskArrayList = new ArrayList<>();
        File file = new File(path);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            while (true) {
                String str;
                if ((str = in.readLine()) != null) {
                    str = str.replaceFirst(System.lineSeparator(), "").replaceAll("_", " ");
                    taskArrayList.add(str);
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "read current plan", e.getMessage());
            portalException.setRequestInformation("Read current plan failed");
            LOGGER.error(portalException.toString());
            shutDownPortal(portalException.toString());
        }
        return taskArrayList;
    }

    /**
     * Init plan list.
     */
    public static void initPlanList() {
        List<String> plan1 = new ArrayList<>();
        plan1.add("start mysql full migration");
        plan1.add("start mysql full migration datacheck");
        planList.put("plan1", plan1);
        List<String> plan2 = new ArrayList<>();
        plan2.add("start mysql full migration");
        plan2.add("start mysql full migration datacheck");
        plan2.add("start mysql incremental migration");
        plan2.add("start mysql incremental migration datacheck");
        planList.put("plan2", plan2);
        List<String> plan3 = new ArrayList<>();
        plan3.add("start mysql full migration");
        plan3.add("start mysql full migration datacheck");
        plan3.add("start mysql incremental migration");
        plan3.add("start mysql incremental migration datacheck");
        plan3.add("start mysql reverse migration");
        planList.put("plan3", plan3);
    }

    /**
     * Show plan list.
     */
    public static void showPlanList() {
        LOGGER.info("plan1");
        List<String> defaultPlan1 = planList.get("plan1");
        for (String task : defaultPlan1) {
            LOGGER.info(task);
        }
        LOGGER.info("plan2");
        List<String> defaultPlan2 = planList.get("plan2");
        for (String task : defaultPlan2) {
            LOGGER.info(task);
        }
        LOGGER.info("plan3");
        List<String> defaultPlan3 = planList.get("plan3");
        for (String task : defaultPlan3) {
            LOGGER.info(task);
        }
    }

    /**
     * Show status.
     */
    public static void showStatus() {
        ArrayList<String> criticalWordList = new ArrayList<>();
        criticalWordList.add("-Dpath=" + PortalControl.portalControlPath);
        criticalWordList.add(Parameter.PORTAL_NAME);
        criticalWordList.add("-Dworkspace.id=" + PortalControl.workspaceId);
        if (ProcessUtils.checkAnotherProcessExist(criticalWordList)) {
            LOGGER.info("Plan " + PortalControl.workspaceId + " is running.");
        } else {
            LOGGER.info("Plan " + PortalControl.workspaceId + " is not running.");
        }
        int status = ChangeStatusTools.getPortalStatus();
        LOGGER.info("Portal status: " + Status.HASHTABLE.get(status));
        if (status < Status.START_INCREMENTAL_MIGRATION) {
            ChangeStatusTools.outputChameleonStatus();
        } else if (status > Status.START_INCREMENTAL_MIGRATION && status < Status.START_REVERSE_MIGRATION) {
            ChangeStatusTools.outputIncrementalStatus(PortalControl.toolsConfigParametersTable.get(Status.INCREMENTAL_PATH));
        } else if (status > Status.START_REVERSE_MIGRATION && status < Status.ERROR) {
            ChangeStatusTools.outputIncrementalStatus(PortalControl.toolsConfigParametersTable.get(Status.REVERSE_PATH));
        }
    }

    /**
     * Init hash table.
     */
    public static void initHashTable() {
        PortalControl.toolsConfigParametersTable.clear();
        PortalControl.toolsMigrationParametersTable.clear();
        PortalControl.initParametersRegexMap();
        getParameterCommandLineFirst(PortalControl.toolsConfigParametersTable, PortalControl.toolsConfigPath);
        PortalControl.initToolsConfigParametersTable();
        getParameterCommandLineFirst(PortalControl.toolsMigrationParametersTable, PortalControl.migrationConfigPath);
    }

    /**
     * Show migration parameters.
     */
    public static void showMigrationParameters() {
        LOGGER.info("Migration parameters:");
        Set<String> parametersSet = new TreeSet<>(toolsMigrationParametersTable.keySet());
        for (String key : parametersSet) {
            if (key.contains("password")) {
                LOGGER.info(key + ":*****");
            } else {
                LOGGER.info(key + ":" + toolsMigrationParametersTable.get(key));
            }
        }
    }

    /**
     * Help.
     */
    public static void help() {
        showParameters();
        LOGGER.info("Order list:");
        LOGGER.info("install_mysql_full_migration_tools_online");
        LOGGER.info("install_mysql_full_migration_tools_offline");
        LOGGER.info("install_mysql_full_migration_tools");
        LOGGER.info("install_mysql_incremental_migration_tools_online");
        LOGGER.info("install_mysql_incremental_migration_tools_offline");
        LOGGER.info("install_mysql_incremental_migration_tools");
        LOGGER.info("install_mysql_datacheck_tools_online");
        LOGGER.info("install_mysql_datacheck_tools_offline");
        LOGGER.info("install_mysql_datacheck_tools");
        LOGGER.info("install_mysql_all_migration_tools");
        LOGGER.info("uninstall_mysql_full_migration_tools");
        LOGGER.info("uninstall_mysql_incremental_migration_tools");
        LOGGER.info("uninstall_mysql_datacheck_tools");
        LOGGER.info("uninstall_mysql_all_migration_tools");
        LOGGER.info("start_mysql_full_migration");
        LOGGER.info("start_mysql_incremental_migration");
        LOGGER.info("start_mysql_reverse_migration");
        LOGGER.info("start_mysql_full_migration_datacheck");
        LOGGER.info("start_mysql_incremental_migration_datacheck");
        LOGGER.info("start_mysql_reverse_migration_datacheck");
        LOGGER.info("start_plan1 --You can execute plan1 in default plan list.");
        LOGGER.info("start_plan2 --You can execute plan2 in default plan list.");
        LOGGER.info("start_plan3 --You can execute plan3 in default plan list.");
        LOGGER.info("start_current_plan --You can execute current plan in currentPlan.");
        LOGGER.info("show_plans --Show default plans.");
        LOGGER.info("show_status --Show plan status.");
        LOGGER.info("show_information --Show information of migration which include user name,password,host,port,"
                + "database name,schema in mysql and openGauss database.");
        LOGGER.info("show_parameters --Show parameters of commandline.");
        LOGGER.info("stop_plan");
    }

    /**
     * Start plan.
     *
     * @param taskList the task list
     */
    public static void startPlan(List<String> taskList) {
        PortalControl.taskList = taskList;
        threadStatusController.setWorkspaceId(workspaceId);
        threadStatusController.start();
        generatePlanHistory(taskList);
        if (!Task.checkPlan(taskList)) {
            LOGGER.error("Invalid plan.");
            return;
        }
        if (taskList.contains("start mysql reverse migration")) {
            boolean canAllowReverseMigration = ReverseMigrationTool.checkReverseMigrationRunnable();
            LogViewUtils.outputInformation(canAllowReverseMigration, "Reverse migration is runnable.",
                    "Reverse migration can not run.");
        }
        String workspaceId = commandLineParameterStringMap.get(Command.Parameters.ID);
        Plan.getInstance(workspaceId).execPlan(taskList);
        threadStatusController.exit = true;
    }

    /**
     * Check path boolean.
     *
     * @return the boolean
     */
    public static boolean checkPath() {
        if (!new File(portalControlPath).exists() || new File(portalControlPath).isFile()) {
            LOGGER.error("portalControlPath not exist");
            return false;
        }
        return true;
    }

    /**
     * Show parameters.
     */
    public static void showParameters() {
        LOGGER.info("Parameters list:");
        LOGGER.info("path  --The path of portal.");
        LOGGER.info("order --The order which portal can execute.");
        LOGGER.info("worksapce.id --The id of migration plan.");
    }

    /**
     * Init command line parameters.
     */
    public static void initCommandLineParameters() {
        commandLineParameterStringMap.clear();
        setCommandLineParameters(Command.Parameters.PATH, "");
        setCommandLineParameters(Command.Parameters.ORDER, "");
        setCommandLineParameters(Command.Parameters.ID, "1");
    }

    private static void setCommandLineParameters(String parameter, String defaultValue) {
        String temp = System.getProperty(parameter);
        if (temp != null && !temp.equals("")) {
            commandLineParameterStringMap.put(parameter, temp);
        } else {
            commandLineParameterStringMap.put(parameter, defaultValue);
        }
    }

    /**
     * Init command handler hash map.
     */
    public static void initValidOrderList() {
        validOrderList.add(Command.Install.Mysql.FullMigration.ONLINE);
        validOrderList.add(Command.Install.Mysql.FullMigration.OFFLINE);
        validOrderList.add(Command.Install.Mysql.FullMigration.DEFAULT);
        validOrderList.add(Command.Install.Mysql.IncrementalMigration.ONLINE);
        validOrderList.add(Command.Install.Mysql.IncrementalMigration.OFFLINE);
        validOrderList.add(Command.Install.Mysql.IncrementalMigration.DEFAULT);
        validOrderList.add(Command.Install.Mysql.ReverseMigration.ONLINE);
        validOrderList.add(Command.Install.Mysql.ReverseMigration.OFFLINE);
        validOrderList.add(Command.Install.Mysql.ReverseMigration.DEFAULT);
        validOrderList.add(Command.Install.Mysql.Check.ONLINE);
        validOrderList.add(Command.Install.Mysql.Check.OFFLINE);
        validOrderList.add(Command.Install.Mysql.Check.DEFAULT);
        validOrderList.add(Command.Install.Mysql.All.DEFAULT);
        validOrderList.add(Command.Install.Mysql.All.ONLINE);
        validOrderList.add(Command.Install.Mysql.All.OFFLINE);
        validOrderList.add(Command.Uninstall.Mysql.FULL);
        validOrderList.add(Command.Uninstall.Mysql.INCREMENTAL);
        validOrderList.add(Command.Uninstall.Mysql.CHECK);
        validOrderList.add(Command.Uninstall.Mysql.REVERSE);
        validOrderList.add(Command.Uninstall.Mysql.ALL);
        validOrderList.add(Command.Start.Mysql.FULL);
        validOrderList.add(Command.Start.Mysql.INCREMENTAL);
        validOrderList.add(Command.Start.Mysql.REVERSE);
        validOrderList.add(Command.Start.Mysql.FULL_CHECK);
        validOrderList.add(Command.Start.Mysql.INCREMENTAL_CHECK);
        validOrderList.add(Command.PREPARE);
        validOrderList.add(Command.Start.Plan.PLAN1);
        validOrderList.add(Command.Start.Plan.PLAN2);
        validOrderList.add(Command.Start.Plan.PLAN3);
        validOrderList.add(Command.Start.Plan.CURRENT);
        validOrderList.add(Command.Start.KAFKA);
        validOrderList.add(Command.HELP);
        validOrderList.add(Command.Show.PLAN);
        validOrderList.add(Command.Show.STATUS);
        validOrderList.add(Command.Show.INFORMATION);
        validOrderList.add(Command.Show.PARAMETERS);
        validOrderList.add(Command.Stop.PLAN);
        validOrderList.add(Command.Stop.INCREMENTAL_MIGRATION);
        validOrderList.add(Command.Stop.REVERSE_MIGRATION);
        validOrderList.add(Command.Stop.KAFKA);
        validOrderList.add(Command.Run.INCREMENTAL_MIGRATION);
        validOrderList.add(Command.Run.REVERSE_MIGRATION);
        validOrderList.add(Command.CheckPortalStatus.CHECK_POTAL_STATUS);
        validOrderList.add(Command.LoadToolsConfig.LOAD_TOOLS_CONFIG);
        validOrderList.add(Command.Verify.VERIFY_PRE_MIGRATION);
        validOrderList.add(Command.Verify.VERIFY_REVERSE_MIGRATION);
    }

    /**
     * Init parameters regex map.
     */
    public static void initParametersRegexMap() {
        parametersRegexMap.put(Chameleon.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Chameleon.VENV_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Chameleon.PKG_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Chameleon.PKG_NAME, Regex.PKG_NAME);
        parametersRegexMap.put(Chameleon.PKG_URL, Regex.URL);
        parametersRegexMap.put(Debezium.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.PKG_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Confluent.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Confluent.PKG_NAME, Regex.PKG_NAME);
        parametersRegexMap.put(Debezium.Confluent.PKG_URL, Regex.URL);
        parametersRegexMap.put(Debezium.Connector.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Connector.MYSQL_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Connector.OPENGAUSS_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Connector.MYSQL_PKG_NAME, Regex.PKG_NAME);
        parametersRegexMap.put(Debezium.Connector.MYSQL_PKG_URL, Regex.URL);
        parametersRegexMap.put(Debezium.Connector.OPENGAUSS_PKG_NAME, Regex.PKG_NAME);
        parametersRegexMap.put(Debezium.Connector.OPENGAUSS_PKG_URL, Regex.URL);
        parametersRegexMap.put(Check.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Check.INSTALL_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Check.PKG_NAME, Regex.PKG_NAME);
        parametersRegexMap.put(Check.PKG_PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Check.PKG_URL, Regex.URL);
        parametersRegexMap.put(Mysql.DATABASE_NAME, Regex.NAME);
        parametersRegexMap.put(Mysql.DATABASE_HOST, Regex.IP);
        parametersRegexMap.put(Mysql.DATABASE_PORT, Regex.PORT);
        parametersRegexMap.put(Mysql.PASSWORD, Regex.NAME);
        parametersRegexMap.put(Mysql.USER, Regex.NAME);
        parametersRegexMap.put(Opengauss.USER, Regex.NAME);
        parametersRegexMap.put(Opengauss.PASSWORD, Regex.NAME);
        parametersRegexMap.put(Opengauss.DATABASE_SCHEMA, Regex.NAME);
        parametersRegexMap.put(Opengauss.DATABASE_NAME, Regex.NAME);
        parametersRegexMap.put(Opengauss.DATABASE_PORT, Regex.PORT);
        parametersRegexMap.put(Opengauss.DATABASE_HOST, Regex.IP);
        parametersRegexMap.put(Offset.FILE, Regex.OFFSET_FILE);
        parametersRegexMap.put(Offset.GTID, Regex.OFFSET_GTID);
        parametersRegexMap.put(Offset.POSITION, Regex.POSITION);
    }

    /**
     * Init tools config parameters table.
     */
    public static void initToolsConfigParametersTable() {
        WorkspacePath workspacePath = WorkspacePath.getInstance(portalControlPath, workspaceId);
        String workPath = PortalControl.portalWorkSpacePath;
        String workConfigDebeziumPath = PathUtils.combainPath(false, workspacePath.getWorkspaceConfigPath(),
                "debezium");
        String workConfigDataCheckPath = PathUtils.combainPath(false, workspacePath.getWorkspaceConfigPath(),
                "datacheck");
        toolsConfigParametersTable.put(Debezium.CONFIG_PATH, workConfigDebeziumPath);
        toolsConfigParametersTable.put(Debezium.Connector.CONFIG_PATH, workConfigDebeziumPath + "connect-avro"
                + "-standalone.properties");
        toolsConfigParametersTable.put(Debezium.Source.CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro"
                + "-standalone-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro"
                + "-standalone-sink.properties");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro"
                + "-standalone-reverse-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro"
                + "-standalone-reverse-sink.properties");
        toolsConfigParametersTable.put(Debezium.Source.INCREMENTAL_CONFIG_PATH, workConfigDebeziumPath + "mysql"
                + "-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.INCREMENTAL_CONFIG_PATH, workConfigDebeziumPath + "mysql-sink"
                + ".properties");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_CONFIG_PATH, workConfigDebeziumPath + "opengauss"
                + "-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_CONFIG_PATH, workConfigDebeziumPath + "opengauss-sink"
                + ".properties");
        toolsConfigParametersTable.put(Check.CONFIG_PATH, workConfigDataCheckPath + "application.yml");
        toolsConfigParametersTable.put(Check.Source.CONFIG_PATH, workConfigDataCheckPath + "application-source.yml");
        toolsConfigParametersTable.put(Check.Sink.CONFIG_PATH, workConfigDataCheckPath + "application-sink.yml");
        toolsConfigParametersTable.put(Check.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2.xml");
        toolsConfigParametersTable.put(Check.Source.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2source.xml");
        toolsConfigParametersTable.put(Check.Sink.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2sink.xml");
        String statusFolder = workspacePath.getWorkspaceStatusPath();
        toolsConfigParametersTable.put(Status.FOLDER, PathUtils.combainPath(false, statusFolder));
        toolsConfigParametersTable.put(Status.INCREMENTAL_FOLDER, PathUtils.combainPath(false, statusFolder,
                "incremental"));
        toolsConfigParametersTable.put(Status.REVERSE_FOLDER, PathUtils.combainPath(false, statusFolder, "reverse"));
        toolsConfigParametersTable.put(Status.PORTAL_PATH, PathUtils.combainPath(true, statusFolder, "portal.txt"));
        toolsConfigParametersTable.put(Status.FULL_PATH, PathUtils.combainPath(true, statusFolder, "full_migration"
                + ".txt"));
        toolsConfigParametersTable.put(Status.FULL_CHECK_PATH, PathUtils.combainPath(true, statusFolder,
                "full_migration_datacheck.txt"));
        toolsConfigParametersTable.put(Status.INCREMENTAL_PATH, PathUtils.combainPath(true, statusFolder,
                "incremental_migration.txt"));
        toolsConfigParametersTable.put(Status.REVERSE_PATH, PathUtils.combainPath(true, statusFolder,
                "reverse_migration.txt"));
        toolsConfigParametersTable.put(Status.XLOG_PATH, PathUtils.combainPath(true, statusFolder, "xlog.txt"));
        String checkLogFolder = PathUtils.combainPath(false, workspacePath.getWorkspaceLogPath(), "datacheck");
        toolsConfigParametersTable.put(Check.LOG_FOLDER, checkLogFolder);
        toolsConfigParametersTable.put(Check.LOG_PATH, checkLogFolder + "check.log");
        toolsConfigParametersTable.put(Check.Source.LOG_PATH, checkLogFolder + "source.log");
        toolsConfigParametersTable.put(Check.Sink.LOG_PATH, checkLogFolder + "sink.log");
        toolsConfigParametersTable.put(Check.Result.FULL, PathUtils.combainPath(false, workPath + "check_result"));
        toolsConfigParametersTable.put(Check.Result.FULL_CURRENT, PathUtils.combainPath(false, workPath
                + "check_result", "result"));
        toolsConfigParametersTable.put(Check.Result.INCREMENTAL, PathUtils.combainPath(false, workPath
                + "check_result", "incremental"));
        toolsConfigParametersTable.put(Check.Result.REVERSE, PathUtils.combainPath(false, workPath + "check_result",
                "reverse"));
        String venvPath = toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        toolsConfigParametersTable.put(Chameleon.RUNNABLE_FILE_PATH, PathUtils.combainPath(true, venvPath + "venv",
                "bin", "chameleon"));
        toolsConfigParametersTable.put(Chameleon.CONFIG_PATH, PathUtils.combainPath(true,
                workspacePath.getWorkspaceConfigPath(), "chameleon", "default_" + workspaceId + ".yml"));
        toolsConfigParametersTable.put(Chameleon.LOG_PATH, PathUtils.combainPath(true,
                workspacePath.getWorkspaceLogPath(), "full_migration.log"));
        toolsConfigParametersTable.put(Parameter.INPUT_ORDER_PATH, PathUtils.combainPath(true,
                workspacePath.getWorkspaceConfigPath(), "input"));
        String workLogDebeziumPath = PathUtils.combainPath(false, workspacePath.getWorkspaceLogPath(), "debezium");
        toolsConfigParametersTable.put(Debezium.LOG_PATH, workLogDebeziumPath);
        toolsConfigParametersTable.put(Debezium.Source.LOG_PATH, workLogDebeziumPath + "connect_source.log");
        toolsConfigParametersTable.put(Debezium.Sink.LOG_PATH, workLogDebeziumPath + "connect_sink.log");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_LOG_PATH, workLogDebeziumPath
                + "reverse_connect_source.log");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_LOG_PATH, workLogDebeziumPath + "reverse_connect_sink"
                + ".log");
        toolsConfigParametersTable.put(Parameter.ERROR_PATH, PathUtils.combainPath(true,
                workspacePath.getWorkspaceLogPath(), "error.log"));
        initToolsConfigParametersTableConfluent();
    }

    /**
     * initialize TheConfluentConfiguration Parameters
     */
    public static void initToolsConfigParametersTableConfluent() {
        String confluentPath = toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        toolsConfigParametersTable.put(Debezium.Zookeeper.CONFIG_PATH,
                PathUtils.combainPath(true, confluentPath
                        + "etc", "kafka", "zookeeper.properties"));
        toolsConfigParametersTable.put(Debezium.Kafka.CONFIG_PATH,
                PathUtils.combainPath(true, confluentPath + "etc",
                        "kafka", "server.properties"));
        toolsConfigParametersTable.put(Debezium.Registry.CONFIG_PATH,
                PathUtils.combainPath(true, confluentPath
                        + "etc", "schema-registry", "schema-registry.properties"));
        String portalPath = PortalControl.portalControlPath;
        toolsConfigParametersTable.put(Debezium.Zookeeper.TMP_PATH,
                PathUtils.combainPath(true, portalPath + "tmp",
                        "zookeeper"));
        toolsConfigParametersTable.put(Debezium.Kafka.TMP_PATH,
                PathUtils.combainPath(true, portalPath + "tmp",
                        "kafka-logs"));
        toolsConfigParametersTable.put(Debezium.Confluent.CONFIG_PATH,
                PathUtils.combainPath(true, confluentPath
                        + "etc", "schema-registry", "schema-registry.properties"));
        toolsConfigParametersTable.put(Debezium.Connector.LOG_PATTERN_PATH, PathUtils.combainPath(true,
                confluentPath + "etc", "kafka", "connect-log4j.properties"));
        String confluentLogPath = PathUtils.combainPath(false, confluentPath, "logs");
        toolsConfigParametersTable.put(Debezium.Zookeeper.LOG_PATH, confluentLogPath + "server.log");
        toolsConfigParametersTable.put(Debezium.Kafka.LOG_PATH, confluentLogPath + "server.log");
        toolsConfigParametersTable.put(Debezium.Registry.LOG_PATH, confluentLogPath + "schema-registry.log");
    }

    /**
     * Init portal path.
     */
    public static void initPortalPath() {
        String path = commandLineParameterStringMap.get(Command.Parameters.PATH);
        if (!new File(path).exists() || new File(path).isFile()) {
            LOGGER.error("portalControlPath not exist");
            return;
        }
        String workspaceId = commandLineParameterStringMap.get(Command.Parameters.ID);
        portalControlPath = path;
        portalErrorPath = PathUtils.combainPath(true, portalControlPath + "logs", "error.log");
        if (workspaceId.equals("")) {
            portalWorkSpacePath = path;
        } else {
            PortalControl.workspaceId = workspaceId;
            portalWorkSpacePath = PathUtils.combainPath(false, path + "workspace", workspaceId);
        }
        toolsConfigPath = PathUtils.combainPath(true, portalWorkSpacePath + "config", "toolspath.properties");
        migrationConfigPath = PathUtils.combainPath(true, portalWorkSpacePath + "config", "migrationConfig.properties");
    }

    /**
     * Generate plan history.
     *
     * @param taskList the task list
     */
    public static void generatePlanHistory(List<String> taskList) {
        String planHistoryFilePath = PathUtils.combainPath(true, PortalControl.portalControlPath + "logs",
                "planHistory.log");
        File file = new File(planHistoryFilePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss");
            ArrayList<String> planInforamtionPatrs = new ArrayList<>();
            planInforamtionPatrs.add(dateFormat.format(date));
            planInforamtionPatrs.add("Current plan: ");
            planInforamtionPatrs.addAll(taskList);
            for (String str : planInforamtionPatrs) {
                LOGGER.info(str);
            }
            StringBuilder planInformation = new StringBuilder();
            for (String str : planInforamtionPatrs) {
                planInformation.append(str).append(System.lineSeparator());
            }
            FileUtils.writeFile(planInformation.toString(), planHistoryFilePath, true);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "generating plan history",
                    e.getMessage());
            portalException.setRequestInformation("Generating plan history failed");
            LOGGER.error(portalException.toString());
        }
    }

    /**
     * Gets parameter command line first.
     *
     * @param hashtable the hashtable
     * @param path      the path
     */
    public static void getParameterCommandLineFirst(Hashtable<String, String> hashtable, String path) {
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            Properties pps = new Properties();
            try {
                pps.load(new FileInputStream(path));
            } catch (IOException e) {
                PortalException portalException = new PortalException("IO exception", "loading the parameters in file"
                        + " " + path, e.getMessage());
                LOGGER.error(portalException.toString());
                shutDownPortal(portalException.toString());
                return;
            }
            for (Object key : pps.keySet()) {
                String keyString = String.valueOf(key);
                String valueString = System.getProperty(keyString);
                if (valueString == null) {
                    valueString = pps.getProperty(keyString);
                }
                if (keyString.contains("path") && !valueString.endsWith(File.separator)) {
                    valueString += File.separator;
                }
                hashtable.put(keyString, valueString);
            }
            pps.clear();
            for (String key : hashtable.keySet()) {
                String valueString = hashtable.get(key);
                hashtable.replace(key, ParamsUtils.changeValue(valueString, hashtable));
            }
            PropertitesUtils.changePropertiesParameters(hashtable, path);
        }
    }

    /**
     * Shut down portal.
     *
     * @param str the str
     */
    public static void shutDownPortal(String str) {
        Plan.stopPlan = true;
        status = Status.ERROR;
        errorMsg = str;
    }

    /**
     * The interface Method runner.
     */
    public interface MethodRunner {
        /**
         * Run method.
         *
         * @param str the str
         */
        void runMethod(String str);
    }
}