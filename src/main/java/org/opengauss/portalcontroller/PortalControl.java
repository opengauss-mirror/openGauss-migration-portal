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

import org.opengauss.portalcontroller.check.CheckTask;
import org.opengauss.portalcontroller.check.CheckTaskIncrementalDatacheck;
import org.opengauss.portalcontroller.check.CheckTaskIncrementalMigration;
import org.opengauss.portalcontroller.check.CheckTaskMysqlFullMigration;
import org.opengauss.portalcontroller.check.CheckTaskReverseMigration;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Offset;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Regex;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Software;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.status.ThreadStatusController;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The type Portal control.
 */
public class PortalControl {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PortalControl.class);

    /**
     * The constant taskList.
     */
    public static List<String> taskList = new ArrayList<>() {

    };

    /**
     * The constant planList.
     */
    public static Hashtable<String, List<String>> planList = new Hashtable<>() {
    };

    /**
     * The constant actionHandlerHashMap.
     */
    public static HashMap<String, EventHandler> actionHandlerHashMap = new HashMap<>();

    /**
     * The constant commandHandlerHashMap.
     */
    public static HashMap<String, EventHandler> commandHandlerHashMap = new HashMap<>();

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
     * The constant threadCheckProcess.
     */
    public static ThreadCheckProcess threadCheckProcess = new ThreadCheckProcess();

    /**
     * The constant commandLineParameterStringMap.
     */
    public static HashMap<String, String> commandLineParameterStringMap = new HashMap<>();

    /**
     * The constant commandCounts.
     */
    public static int commandCounts = 0;

    /**
     * The constant latestCommand.
     */
    public static String latestCommand = "";

    /**
     * The constant status.
     */
    public static int status = Status.START_FULL_MIGRATION;

    /**
     * The constant fullDatacheckFinished.
     */
    public static boolean fullDatacheckFinished = false;

    /**
     * The constant allowReverseMigration.
     */
    public static boolean allowReverseMigration = false;

    /**
     * The constant refuseReverseMigrationReason.
     */
    public static String refuseReverseMigrationReason = "";

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
     * The constant softwareList.
     */
    public static ArrayList<Software> softwareList = new ArrayList<>();

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        initPlanList();
        initParametersRegexMap();
        initCommandLineParameters();
        initCommandHandlerHashMap();
        String path = commandLineParameterStringMap.get(Command.Parameters.PATH);
        String workspaceId = commandLineParameterStringMap.get(Command.Parameters.ID);
        if (!workspaceId.equals("")) {
            PortalControl.workspaceId = workspaceId;
        }
        portalControlPath = path;
        portalErrorPath = PathUtils.combainPath(true, portalControlPath + "logs", "error.log");
        if (workspaceId.equals("")) {
            portalWorkSpacePath = path;
        } else {
            portalWorkSpacePath = PathUtils.combainPath(false, path + "workspace", workspaceId);
        }
        toolsConfigPath = PathUtils.combainPath(true, portalWorkSpacePath + "config", "toolspath.properties");
        migrationConfigPath = PathUtils.combainPath(true, portalWorkSpacePath + "config", "migrationConfig.properties");
        Plan.createWorkspace(workspaceId);
        checkPath();
        Task.initTaskProcessMap();
        Task.initTaskLogMap();
        threadCheckProcess.setName("threadCheckProcess");
        threadCheckProcess.start();
        Tools.cleanInputOrder();
        threadGetOrder.start();
        String order = commandLineParameterStringMap.get(Command.Parameters.ORDER);
        if (order != null) {
            String[] orders = order.split("_");
            String newOrder = orders[0];
            for (int i = 1; i < orders.length; i++) {
                newOrder += " " + orders[i];
            }
            if (commandHandlerHashMap.containsKey(newOrder)) {
                EventHandler eventHandler = commandHandlerHashMap.get(newOrder);
                eventHandler.handle(newOrder);
            } else {
                LOGGER.error("Invalid command.Please input help to get valid command.");
            }
        } else {
            LOGGER.error("Invalid command.Please input help to get valid command.");
        }
        threadCheckProcess.exit = true;
        threadGetOrder.exit = true;
        threadStatusController.exit = true;
    }

    /**
     * Init tasklist.
     *
     * @param path the path
     */
    public static void initTasklist(String path) {
        File file = new File(path);
        String str = "";
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while (true) {
                str = in.readLine();
                if (str != null) {
                    str = str.replaceFirst(System.lineSeparator(), "");
                    str = str.replaceAll("_", " ");
                    taskList.add(str);
                } else {
                    break;
                }
            }
            in.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception","read current plan",e.getMessage());
            portalException.setRequestInformation("Read current plan failed");
            portalException.shutDownPortal(LOGGER);
        }
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
        if (Tools.checkAnotherProcessExist(criticalWordList)) {
            LOGGER.info("Plan " + PortalControl.workspaceId + " is running.");
        } else {
            LOGGER.info("Plan " + PortalControl.workspaceId + " is not running.");
        }
        int status = ChangeStatusTools.getPortalStatus(threadStatusController);
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
        Tools.getParameterCommandLineFirst(PortalControl.toolsConfigParametersTable, PortalControl.toolsConfigPath);
        PortalControl.initToolsConfigParametersTable();
        Tools.getParameterCommandLineFirst(PortalControl.toolsMigrationParametersTable, PortalControl.migrationConfigPath);
    }

    /**
     * Stop.
     */
    public static void stop() {
        String stopOrder = "stop";
        String plan = commandLineParameterStringMap.get(Command.Parameters.PARAMETER);
        if (plan.equals("plan")) {
            stopOrder += " plan";
        }
        EventHandler stopEventHandler = commandHandlerHashMap.get(stopOrder);
        if (stopEventHandler != null) {
            LOGGER.info(stopOrder);
            stopEventHandler.handle(stopOrder);
        } else {
            LOGGER.error("Invalid command.");
        }
    }

    /**
     * Show migration parameters.
     */
    public static void showMigrationParameters() {
        LOGGER.info("Migration parameters:");
        Set<String> parametersSet = new TreeSet<String>((o1, o2) -> (o1.compareTo(o2)));
        parametersSet.addAll(toolsMigrationParametersTable.keySet());
        for (String key : parametersSet) {
            if (key.contains("password")) {
                LOGGER.info(key + ":*****");
            } else {
                LOGGER.info(key + ":" + toolsMigrationParametersTable.get(key));
            }
        }
    }

    /**
     * Install.
     */
    public static void install() {
        String installOrder = "install";
        String type = commandLineParameterStringMap.get(Command.Parameters.TYPE);
        installOrder += " " + type;
        String migrationType = commandLineParameterStringMap.get(Command.Parameters.MIGRATION_TYPE);
        if (commandLineParameterStringMap.get(Command.Parameters.CHECK).equals("true")) {
            installOrder += " datacheck";
        } else {
            installOrder += " " + migrationType + " migration";
        }
        installOrder += " tools";
        String parameter = commandLineParameterStringMap.get(Command.Parameters.PARAMETER);
        if (parameter.equals("online") || parameter.equals("offline")) {
            installOrder += " " + parameter;
        }
        EventHandler installEventHandler = commandHandlerHashMap.get(installOrder);
        if (installEventHandler != null) {
            LOGGER.info(installOrder);
            installEventHandler.handle(installOrder);
        } else {
            LOGGER.error("Invalid command.");
        }
    }

    /**
     * Uninstall.
     */
    public static void uninstall() {
        String uninstallOrder = "uninstall";
        String migrationType = commandLineParameterStringMap.get(Command.Parameters.MIGRATION_TYPE);
        String type = commandLineParameterStringMap.get(Command.Parameters.TYPE);
        uninstallOrder += " " + type;
        if (commandLineParameterStringMap.get(Command.Parameters.CHECK).equals("true")) {
            uninstallOrder += " datacheck";
        } else {
            uninstallOrder += " " + migrationType + " migration";
        }
        uninstallOrder += " tools";
        EventHandler uninstallEventHandler = commandHandlerHashMap.get(uninstallOrder);
        if (uninstallEventHandler != null) {
            LOGGER.info(uninstallOrder);
            uninstallEventHandler.handle(uninstallOrder);
        } else {
            LOGGER.error("Invalid command.");
        }
    }

    /**
     * Start.
     */
    public static void start() {
        String startOrder = "start";
        String plan = commandLineParameterStringMap.get(Command.Parameters.PARAMETER);
        if (planList.containsKey(plan)) {
            startOrder += " " + plan;
        } else if (plan.equals("current")) {
            startOrder += " " + plan + " plan";
        } else {
            String type = commandLineParameterStringMap.get(Command.Parameters.TYPE);
            String migrationType = commandLineParameterStringMap.get(Command.Parameters.MIGRATION_TYPE);
            startOrder += " " + type + " " + migrationType + " migration";
            if (commandLineParameterStringMap.get(Command.Parameters.CHECK).equals("true")) {
                startOrder += " datacheck";
            }
        }
        EventHandler startEventHandler = commandHandlerHashMap.get(startOrder);
        if (startEventHandler != null) {
            LOGGER.info(startOrder);
            startEventHandler.handle(startOrder);
        } else {
            LOGGER.error("Invalid command.");
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
        LOGGER.info("start_current plan --You can execute current plan in currentPlan.");
        LOGGER.info("show_plans --Show default plans.");
        LOGGER.info("show_status --Show plan status.");
        LOGGER.info("show_information --Show information of migration which include user name,password,host,port,database name,schema in mysql and openGauss database.");
        LOGGER.info("show_parameters --Show parameters of commandline.");
        LOGGER.info("stop_plan");
    }

    /**
     * Start default plan.
     *
     * @param plan the plan
     */
    public static void startDefaultPlan(String plan) {
        if (!Plan.isPlanRunnable) {
            LOGGER.error("There is a plan already running.");
            return;
        }
        if (planList.containsKey(plan)) {
            taskList.addAll(planList.get(plan));
            startPlan();
        } else {
            LOGGER.error("Default plan list don't have plan whose name is " + plan + ".");
        }
    }

    /**
     * Start current plan.
     */
    public static void startCurrentPlan() {
        if (!Plan.isPlanRunnable) {
            LOGGER.error("There is a plan already running.");
            return;
        }
        String path = PortalControl.portalControlPath + "config/currentPlan";
        initTasklist(path);
        startPlan();
    }

    /**
     * Start single task plan.
     *
     * @param task the task
     */
    public static void startSingleTaskPlan(String task) {
        if (!Plan.isPlanRunnable) {
            LOGGER.error("There is a plan already running.");
            return;
        }
        taskList.add(task);
        startPlan();
    }

    /**
     * Start plan.
     */
    public static void startPlan() {
        threadStatusController.setWorkspaceId(workspaceId);
        threadStatusController.start();
        String workspaceId = commandLineParameterStringMap.get(Command.Parameters.ID);
        Tools.generatePlanHistory(taskList);
        if (!Task.checkPlan(taskList)) {
            Plan.installPlanPackages();
            LOGGER.error("Invalid plan.");
            return;
        }
        if (taskList.contains("start mysql reverse migration")) {
            boolean flag = Tools.checkReverseMigrationRunnable();
            Tools.outputInformation(flag, "Reverse migration is runnable.", "Reverse migration can not run.");
            PortalControl.allowReverseMigration = flag;
        }
        Plan.getInstance(workspaceId).execPlan(PortalControl.taskList);
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
    public static void initCommandHandlerHashMap() {
        ArrayList<CheckTask> checkTasks = new ArrayList<>();
        ArrayList<String> installWays = new ArrayList<>();
        CheckTask checkTaskMysqlFullMigration = new CheckTaskMysqlFullMigration();
        checkTasks.add(checkTaskMysqlFullMigration);
        installWays.add(MigrationParameters.Install.FULL_MIGRATION);
        CheckTask checkTaskMysqlIncrementalMigration = new CheckTaskIncrementalMigration();
        checkTasks.add(checkTaskMysqlIncrementalMigration);
        installWays.add(MigrationParameters.Install.INCREMENTAL_MIGRATION);
        CheckTask checkTaskMysqlReverseMigration = new CheckTaskReverseMigration();
        checkTasks.add(checkTaskMysqlReverseMigration);
        installWays.add(MigrationParameters.Install.REVERSE_MIGRATION);
        CheckTask checkTaskDatacheck = new CheckTaskIncrementalDatacheck();
        checkTasks.add(checkTaskDatacheck);
        installWays.add(MigrationParameters.Install.CHECK);
        commandHandlerHashMap.put(Command.Install.Mysql.FullMigration.ONLINE, (event) -> checkTaskMysqlFullMigration.installAllPackages(true));
        commandHandlerHashMap.put(Command.Install.Mysql.FullMigration.OFFLINE, (event) -> checkTaskMysqlFullMigration.installAllPackages(false));
        commandHandlerHashMap.put(Command.Install.Mysql.FullMigration.DEFAULT, (event) -> checkTaskMysqlFullMigration.installAllPackages());
        commandHandlerHashMap.put(Command.Install.Mysql.IncrementalMigration.ONLINE, (event) -> checkTaskMysqlIncrementalMigration.installAllPackages(true));
        commandHandlerHashMap.put(Command.Install.Mysql.IncrementalMigration.OFFLINE, (event) -> checkTaskMysqlIncrementalMigration.installAllPackages(false));
        commandHandlerHashMap.put(Command.Install.Mysql.IncrementalMigration.DEFAULT, (event) -> checkTaskMysqlIncrementalMigration.installAllPackages());
        commandHandlerHashMap.put(Command.Install.Mysql.ReverseMigration.ONLINE, (event) -> checkTaskMysqlReverseMigration.installAllPackages(true));
        commandHandlerHashMap.put(Command.Install.Mysql.ReverseMigration.OFFLINE, (event) -> checkTaskMysqlReverseMigration.installAllPackages(false));
        commandHandlerHashMap.put(Command.Install.Mysql.ReverseMigration.DEFAULT, (event) -> checkTaskMysqlReverseMigration.installAllPackages());
        commandHandlerHashMap.put(Command.Install.Mysql.Check.ONLINE, (event) -> checkTaskDatacheck.installAllPackages(true));
        commandHandlerHashMap.put(Command.Install.Mysql.Check.OFFLINE, (event) -> checkTaskDatacheck.installAllPackages(false));
        commandHandlerHashMap.put(Command.Install.Mysql.Check.DEFAULT, (event) -> checkTaskDatacheck.installAllPackages());
        commandHandlerHashMap.put(Command.Install.Mysql.All.DEFAULT, (event) -> InstallMigrationTools.installAllMigrationTools(checkTasks));
        commandHandlerHashMap.put(Command.Install.Mysql.All.ONLINE, (event) -> InstallMigrationTools.installAllMigrationTools(true, checkTasks));
        commandHandlerHashMap.put(Command.Install.Mysql.All.OFFLINE, (event) -> InstallMigrationTools.installAllMigrationTools(false, checkTasks));
        commandHandlerHashMap.put(Command.Uninstall.Mysql.FULL, (event) -> checkTaskMysqlFullMigration.uninstall());
        commandHandlerHashMap.put(Command.Uninstall.Mysql.INCREMENTAL, (event) -> checkTaskMysqlIncrementalMigration.uninstall());
        commandHandlerHashMap.put(Command.Uninstall.Mysql.CHECK, (event) -> checkTaskDatacheck.uninstall());
        commandHandlerHashMap.put(Command.Uninstall.Mysql.REVERSE, (event) -> checkTaskMysqlReverseMigration.uninstall());
        commandHandlerHashMap.put(Command.Uninstall.Mysql.ALL, (event) -> InstallMigrationTools.uninstallMigrationTools());
        commandHandlerHashMap.put(Command.Start.Mysql.FULL, (event) -> startSingleTaskPlan(Command.Start.Mysql.FULL));
        commandHandlerHashMap.put(Command.Start.Mysql.INCREMENTAL, (event) -> startSingleTaskPlan(Command.Start.Mysql.INCREMENTAL));
        commandHandlerHashMap.put(Command.Start.Mysql.REVERSE, (event) -> startSingleTaskPlan(Command.Start.Mysql.REVERSE));
        commandHandlerHashMap.put(Command.Start.Mysql.FULL_CHECK, (event) -> startSingleTaskPlan(Command.Start.Mysql.FULL_CHECK));
        commandHandlerHashMap.put(Command.Start.Mysql.INCREMENTAL_CHECK, (event) -> startSingleTaskPlan(Command.Start.Mysql.INCREMENTAL_CHECK));
        commandHandlerHashMap.put(Command.Start.Plan.PLAN1, (event) -> startDefaultPlan("plan1"));
        commandHandlerHashMap.put(Command.Start.Plan.PLAN2, (event) -> startDefaultPlan("plan2"));
        commandHandlerHashMap.put(Command.Start.Plan.PLAN3, (event) -> startDefaultPlan("plan3"));
        commandHandlerHashMap.put(Command.Start.Plan.CURRENT, (event) -> startCurrentPlan());
        commandHandlerHashMap.put(Command.HELP, (event) -> help());
        commandHandlerHashMap.put(Command.Show.PLAN, (event) -> showPlanList());
        commandHandlerHashMap.put(Command.Show.STATUS, (event) -> showStatus());
        commandHandlerHashMap.put(Command.Show.INFORMATION, (event) -> showMigrationParameters());
        commandHandlerHashMap.put(Command.Show.PARAMETERS, (event) -> showParameters());
        commandHandlerHashMap.put(Command.Stop.PLAN, (event) -> Tools.writeInputOrder(Command.Stop.PLAN));
        commandHandlerHashMap.put(Command.Stop.INCREMENTAL_MIGRATION, (event) -> Tools.writeInputOrder(Command.Stop.INCREMENTAL_MIGRATION));
        commandHandlerHashMap.put(Command.Stop.REVERSE_MIGRATION, (event) -> Tools.writeInputOrder(Command.Stop.REVERSE_MIGRATION));
        commandHandlerHashMap.put(Command.Run.INCREMENTAL_MIGRATION, (event) -> Tools.writeInputOrder(Command.Run.INCREMENTAL_MIGRATION));
        commandHandlerHashMap.put(Command.Run.REVERSE_MIGRATION, (event) -> Tools.writeInputOrder(Command.Run.REVERSE_MIGRATION));
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
        parametersRegexMap.put(Debezium.Kafka.PATH, Regex.FOLDER_PATH);
        parametersRegexMap.put(Debezium.Kafka.PKG_URL, Regex.URL);
        parametersRegexMap.put(Debezium.Kafka.PKG_NAME, Regex.PKG_NAME);
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
        String kafkaPath = toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        String confluentPath = toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        String workPath = PortalControl.portalWorkSpacePath;
        String workConfigDebeziumPath = PathUtils.combainPath(false, workspacePath.getWorkspaceConfigPath(), "debezium");
        String workConfigDataCheckPath = PathUtils.combainPath(false, workspacePath.getWorkspaceConfigPath(), "datacheck");
        String portalPath = PortalControl.portalControlPath;
        toolsConfigParametersTable.put(Debezium.Zookeeper.CONFIG_PATH, PathUtils.combainPath(true, kafkaPath + "config", "zookeeper.properties"));
        toolsConfigParametersTable.put(Debezium.Kafka.CONFIG_PATH, PathUtils.combainPath(true, kafkaPath + "config", "server.properties"));
        toolsConfigParametersTable.put(Debezium.Registry.CONFIG_PATH, PathUtils.combainPath(true, confluentPath + "etc", "schema-registry", "schema-registry.properties"));
        toolsConfigParametersTable.put(Debezium.Zookeeper.TMP_PATH, PathUtils.combainPath(true, portalPath + "tmp", "zookeeper"));
        toolsConfigParametersTable.put(Debezium.Kafka.TMP_PATH, PathUtils.combainPath(true, portalPath + "tmp", "kafka-logs"));
        toolsConfigParametersTable.put(Debezium.Confluent.CONFIG_PATH, PathUtils.combainPath(true, confluentPath + "etc", "schema-registry", "schema-registry.properties"));
        toolsConfigParametersTable.put(Debezium.CONFIG_PATH, workConfigDebeziumPath);
        toolsConfigParametersTable.put(Debezium.Connector.CONFIG_PATH, workConfigDebeziumPath + "connect-avro-standalone.properties");
        toolsConfigParametersTable.put(Debezium.Source.CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro-standalone-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro-standalone-sink.properties");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro-standalone-reverse-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_CONNECTOR_PATH, workConfigDebeziumPath + "connect-avro-standalone-reverse-sink.properties");
        toolsConfigParametersTable.put(Debezium.Source.INCREMENTAL_CONFIG_PATH, workConfigDebeziumPath + "mysql-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.INCREMENTAL_CONFIG_PATH, workConfigDebeziumPath + "mysql-sink.properties");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_CONFIG_PATH, workConfigDebeziumPath + "opengauss-source.properties");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_CONFIG_PATH, workConfigDebeziumPath + "opengauss-sink.properties");
        toolsConfigParametersTable.put(Check.CONFIG_PATH, workConfigDataCheckPath + "application.yml");
        toolsConfigParametersTable.put(Check.Source.CONFIG_PATH, workConfigDataCheckPath + "application-source.yml");
        toolsConfigParametersTable.put(Check.Sink.CONFIG_PATH, workConfigDataCheckPath + "application-sink.yml");
        toolsConfigParametersTable.put(Check.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2.xml");
        toolsConfigParametersTable.put(Check.Source.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2source.xml");
        toolsConfigParametersTable.put(Check.Sink.LOG_PATTERN_PATH, workConfigDataCheckPath + "log4j2sink.xml");
        String statusFolder = workspacePath.getWorkspaceStatusPath();
        toolsConfigParametersTable.put(Status.FOLDER, PathUtils.combainPath(false, statusFolder));
        toolsConfigParametersTable.put(Status.INCREMENTAL_FOLDER, PathUtils.combainPath(false, statusFolder, "incremental"));
        toolsConfigParametersTable.put(Status.REVERSE_FOLDER, PathUtils.combainPath(false, statusFolder, "reverse"));
        toolsConfigParametersTable.put(Status.PORTAL_PATH, PathUtils.combainPath(true, statusFolder, "portal.txt"));
        toolsConfigParametersTable.put(Status.FULL_PATH, PathUtils.combainPath(true, statusFolder, "full_migration.txt"));
        toolsConfigParametersTable.put(Status.INCREMENTAL_PATH, PathUtils.combainPath(true, statusFolder, "incremental_migration.txt"));
        toolsConfigParametersTable.put(Status.REVERSE_PATH, PathUtils.combainPath(true, statusFolder, "reverse_migration.txt"));
        toolsConfigParametersTable.put(Status.XLOG_PATH, PathUtils.combainPath(true, statusFolder, "xlog.txt"));
        String checkLogFolder = PathUtils.combainPath(false, workspacePath.getWorkspaceLogPath(), "datacheck");
        toolsConfigParametersTable.put(Check.LOG_FOLDER, checkLogFolder);
        toolsConfigParametersTable.put(Check.LOG_PATH, checkLogFolder + "check.log");
        toolsConfigParametersTable.put(Check.Source.LOG_PATH, checkLogFolder + "source.log");
        toolsConfigParametersTable.put(Check.Sink.LOG_PATH, checkLogFolder + "sink.log");
        toolsConfigParametersTable.put(Check.Result.FULL, PathUtils.combainPath(false, workPath + "check_result"));
        toolsConfigParametersTable.put(Check.Result.INCREMENTAL, PathUtils.combainPath(false, workPath + "check_result", "incremental"));
        toolsConfigParametersTable.put(Check.Result.REVERSE, PathUtils.combainPath(false, workPath + "check_result", "reverse"));
        toolsConfigParametersTable.put(Debezium.Connector.LOG_PATTERN_PATH, PathUtils.combainPath(true, confluentPath + "etc", "kafka", "connect-log4j.properties"));
        String venvPath = toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        toolsConfigParametersTable.put(Chameleon.RUNNABLE_FILE_PATH, PathUtils.combainPath(true, venvPath + "venv", "bin", "chameleon"));
        toolsConfigParametersTable.put(Chameleon.CONFIG_PATH, PathUtils.combainPath(true, workspacePath.getWorkspaceConfigPath(), "chameleon", "default_" + workspaceId + ".yml"));
        toolsConfigParametersTable.put(Chameleon.LOG_PATH, PathUtils.combainPath(true, workspacePath.getWorkspaceLogPath(), "full_migration.log"));
        toolsConfigParametersTable.put(Parameter.INPUT_ORDER_PATH, PathUtils.combainPath(true, workspacePath.getWorkspaceConfigPath(), "input"));
        String workLogDebeziumPath = PathUtils.combainPath(false, workspacePath.getWorkspaceLogPath(), "debezium");
        toolsConfigParametersTable.put(Debezium.LOG_PATH, workLogDebeziumPath);
        toolsConfigParametersTable.put(Debezium.Zookeeper.LOG_PATH, workLogDebeziumPath + "server.log");
        toolsConfigParametersTable.put(Debezium.Kafka.LOG_PATH, workLogDebeziumPath + "server.log");
        toolsConfigParametersTable.put(Debezium.Registry.LOG_PATH, workLogDebeziumPath + "schema-registry.log");
        toolsConfigParametersTable.put(Debezium.Source.LOG_PATH, workLogDebeziumPath + "connect_source.log");
        toolsConfigParametersTable.put(Debezium.Sink.LOG_PATH, workLogDebeziumPath + "connect_sink.log");
        toolsConfigParametersTable.put(Debezium.Source.REVERSE_LOG_PATH, workLogDebeziumPath + "reverse_connect_source.log");
        toolsConfigParametersTable.put(Debezium.Sink.REVERSE_LOG_PATH, workLogDebeziumPath + "reverse_connect_sink.log");
        toolsConfigParametersTable.put(Parameter.ERROR_PATH, PathUtils.combainPath(true, workspacePath.getWorkspaceLogPath(), "error.log"));
    }

    /**
     * The interface Event handler.
     */
    public interface EventHandler {
        /**
         * Handle.
         *
         * @param str the str
         */
        void handle(String str);
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