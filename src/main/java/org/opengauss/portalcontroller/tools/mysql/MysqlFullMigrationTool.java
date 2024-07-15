/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.tools.mysql;

import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Regex;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.constant.ToolsConfigEnum;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.status.ChangeStatusTools;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;
import org.opengauss.portalcontroller.utils.YmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;

/**
 * MysqlFullMigrationTool
 *
 * @date :2023/11/3 15:22
 * @description: MysqlFullMigrationTool
 * @version: 1.1
 * @since 1.1
 */
public class MysqlFullMigrationTool extends ParamsConfig implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlFullMigrationTool.class);

    /**
     * The constant shouldDetachReplica.
     */
    public static boolean shouldDetachReplica = true;

    Map<String, Object> configMap = null;

    /**
     * Change full migration parameters.
     */
    @Override
    public void initDataBaseParams() {
        String mysqlDatabaseHost = toolsMigrationParametersTable.get(Mysql.DATABASE_HOST);
        String mysqlDatabasePort = toolsMigrationParametersTable.get(Mysql.DATABASE_PORT);
        String opengaussDatabaseHost = toolsMigrationParametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = toolsMigrationParametersTable.get(Opengauss.DATABASE_PORT);
        if (mysqlDatabaseHost.matches(Regex.IP) && mysqlDatabasePort.matches(Regex.PORT)
                && opengaussDatabaseHost.matches(Regex.IP) && opengaussDatabasePort.matches(Regex.PORT)) {
            configMap.put(Chameleon.Parameters.Mysql.HOST, mysqlDatabaseHost);
            configMap.put(Chameleon.Parameters.Mysql.PORT, mysqlDatabasePort);
            configMap.put(Chameleon.Parameters.Opengauss.HOST, opengaussDatabaseHost);
            configMap.put(Chameleon.Parameters.Opengauss.PORT, opengaussDatabasePort);
            configMap.put(Chameleon.Parameters.Mysql.USER, toolsMigrationParametersTable.get(Mysql.USER));
            configMap.put(Chameleon.Parameters.Mysql.PASSWORD, toolsMigrationParametersTable.get(Mysql.PASSWORD));
            String mysqlDatabaseName = toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
            configMap.put(Chameleon.Parameters.Mysql.NAME, mysqlDatabaseName);
            configMap.put(Chameleon.Parameters.Opengauss.USER, toolsMigrationParametersTable.get(Opengauss.USER));
            configMap.put(Chameleon.Parameters.Opengauss.PASSWORD,
                    toolsMigrationParametersTable.get(Opengauss.PASSWORD));
            String opengaussDatabaseName = toolsMigrationParametersTable.get(Opengauss.DATABASE_NAME);
            configMap.put(Chameleon.Parameters.Opengauss.NAME, opengaussDatabaseName);
            configMap.put(Chameleon.Parameters.Mysql.MAPPING + "." + mysqlDatabaseName,
                    toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA));
            setTables();
        } else {
            LOGGER.error("Invalid parameters.");
        }
    }

    /**
     * set database tables.
     */
    private void setTables() {
        String tableStr = toolsMigrationParametersTable.get(Mysql.DATABASE_TABLE);
        if (!Plan.isRuleEnable(tableStr)) {
            return;
        }
        String[] tableArr = tableStr.split(",");
        configMap.put(Chameleon.Parameters.Mysql.TABLES, Arrays.asList(tableArr));
    }

    /**
     * Write chameleon override type.
     */
    private static void writeChameleonOverrideType() {
        String path = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
        HashMap<String, Object> oldChameleonConfigMap = YmlUtils.getYmlParameters(path);
        if (System.getProperty(Chameleon.Override.AMOUNT) != null) {
            int amount = Integer.parseInt(System.getProperty(Chameleon.Override.AMOUNT));
            oldChameleonConfigMap.remove(Chameleon.Override.AMOUNT);
            for (int i = 1; i <= amount; i++) {
                String tables = System.getProperty(Chameleon.Override.TABLES + i);
                String[] tableArray;
                if (tables.contains(",")) {
                    tableArray = tables.split(",");
                } else {
                    tableArray = new String[]{tables};
                }
                HashMap<String, Object> typeOverrideHashMap = new HashMap<>();
                String sinkType = System.getProperty(Chameleon.Override.SINK_TYPE + i);
                typeOverrideHashMap.put(Chameleon.Override.SINK_TYPE, sinkType);
                typeOverrideHashMap.put(Chameleon.Override.TABLES, tableArray);
                String sourceType = System.getProperty(Chameleon.Override.SOURCE_TYPE + i);
                oldChameleonConfigMap.put(Chameleon.Override.AMOUNT + "." + sourceType, typeOverrideHashMap);
            }
        }
        YmlUtils.changeYmlParameters(oldChameleonConfigMap, path);
    }

    /**
     * install
     *
     * @param isDownload isDownload
     * @return boolean
     */
    @Override
    public boolean install(boolean isDownload) {
        try {
            if (isDownload) {
                RuntimeExecUtils.download(Chameleon.PKG_URL, Chameleon.PKG_PATH);
            }
            Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
            String chameleonInstallPath = hashtable.get(Chameleon.INSTALL_PATH);
            String chameleonVersion = "chameleon-" + hashtable.get(Parameter.TOOL_VERSION);
            String chameleonInstallLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath
                    + "tools", "chameleon", chameleonVersion, "install_chameleon.log");
            FileUtils.createFile(chameleonInstallPath, false);
            String chameleonVersionOrder = hashtable.get(Chameleon.RUNNABLE_FILE_PATH) + " --version";
            if (checkChameleonStatus(chameleonVersionOrder, chameleonInstallLogPath)) {
                LOGGER.info("check chameleon success...");
                return true;
            }
            LOGGER.warn("first check chameleon failed, start install...");
            preInstall();
            String chameleonPkgSpace = "200MB";
            String chameleonPkgPath = hashtable.get(Chameleon.PKG_PATH) + hashtable.get(Chameleon.PKG_NAME);
            RuntimeExecUtils.unzipFile(chameleonPkgPath, chameleonPkgSpace, chameleonInstallPath);
            String buildChameleonName = "install.sh";
            String chameleonVenvPath = hashtable.get(Chameleon.VENV_PATH);
            RuntimeExecUtils.runShell(buildChameleonName, chameleonVenvPath);
            FileUtils.checkFileExist(hashtable.get(Chameleon.RUNNABLE_FILE_PATH), 300);
            checkChameleonVersion(chameleonVersionOrder, chameleonInstallLogPath);
        } catch (PortalException e) {
            LOGGER.error("install failed", e);
            return false;
        }
        return true;
    }


    /**
     * Check chameleon version.
     *
     * @param order                   the order
     * @param chameleonInstallLogPath the chameleon install log path
     * @throws PortalException the portal exception
     */
    public void checkChameleonVersion(String order, String chameleonInstallLogPath) throws PortalException {
        String chameleonTestLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "logs",
                "test_chameleon.log");
        try {
            RuntimeExecUtils.executeOrder(order, 3000, PortalControl.portalControlPath,
                    chameleonTestLogPath, true, new ArrayList<>());
        } catch (PortalException portalException) {
            String logStr = FileUtils.outputFileString(chameleonInstallLogPath);
            if (logStr.isEmpty()) {
                portalException.setRequestInformation("Please check pip download source.");
            } else {
                portalException.setRequestInformation(logStr);
            }
            throw portalException;
        }
        String log = LogViewUtils.getFullLog(chameleonTestLogPath).trim();
        if (log.startsWith("chameleon")) {
            LOGGER.info("Install chameleon success.");
        } else {
            throw new PortalException("Portal exception", "installing chameleon",
                    "Install chameleon failed.Information:" + log);
        }
        RuntimeExecUtils.removeFile(chameleonTestLogPath, PortalControl.portalErrorPath);
    }

    /**
     * first Check chameleon version.
     *
     * @param order                   the order
     * @param chameleonInstallLogPath the chameleon install log path
     * @return boolean
     */
    public boolean checkChameleonStatus(String order, String chameleonInstallLogPath) {
        try {
            checkChameleonVersion(order, chameleonInstallLogPath);
        } catch (PortalException e) {
            return false;
        }
        return true;
    }

    /**
     * prepare install
     */
    private void preInstall() throws PortalException {
        if (InstallMigrationUtils.checkSudoPermission()) {
            InstallMigrationUtils.installDependencies("chameleon");
        } else {
            LOGGER.error("The sudo command cannot be used. Skip installation of dependencies required by chameleon.");
        }
        checkPython3Availability();
    }

    /**
     * check python3 availability
     */
    private void checkPython3Availability() throws PortalException {
        String command = "python3 --version";
        int waitTime = 3000;
        String commandResult = RuntimeExecUtils.executeOrder(command, waitTime);
        if (commandResult.trim().startsWith("Python 3")) {
            LOGGER.info("Python 3 is available on the server.");
        } else {
            LOGGER.error("Python 3 is not available on the server.");
        }
    }

    /**
     * Copy config files.
     *
     * @param workspaceId the workspace id
     * @throws PortalException the portal exception
     */
    public void copyConfigFiles(String workspaceId) throws PortalException {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String chameleonRunnableFilePath = hashtable.get(Chameleon.RUNNABLE_FILE_PATH);
        String chameleonPath = hashtable.get(Chameleon.PATH).replaceFirst("~", System.getProperty("user.home"));
        String fileDirectory = PathUtils.combainPath(false, chameleonPath + "configuration");
        try {
            RuntimeExecUtils.executeOrder(chameleonRunnableFilePath + " set_configuration_files", 3000,
                    PortalControl.portalErrorPath);
        } catch (PortalException e) {
            e.setRequestInformation("Set configuration files failed");
            throw e;
        }
        try {
            String newFileName = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath
                    + "config", "chameleon", "default_" + workspaceId + ".yml");
            FileUtils.createFile(fileDirectory, false);
            RuntimeExecUtils.copyFile(newFileName, fileDirectory, true);
        } catch (PortalException e) {
            e.setRequestInformation("Copy config files failed");
            throw e;
        }
    }

    /**
     * initWorkSpaceParams
     *
     * @param workspaceId workspaceId
     */
    @Override
    public void initWorkSpaceParams(String workspaceId) {
        configMap.put("pid_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "pid"));
        configMap.put("sources.mysql.csv_dir", PathUtils.combainPath(false,
                PortalControl.portalWorkSpacePath + "tmp"));
        configMap.put("sources.mysql.out_dir", PathUtils.combainPath(false,
                PortalControl.portalWorkSpacePath + "tmp"));
        configMap.put("dump_json", "yes");
        if (Strings.isNotBlank(toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL))) {
            configMap.put("log_level",
                    toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL).toLowerCase());
        }
    }

    /**
     *
     * initInteractionParams
     */
    @Override
    void initInteractionParams() {

    }

    /**
     * init
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION;
        }
        LOGGER.info("mysql full migration tool start init");
        try {
            String chameleonConfigOldPath = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath
                    + "config", "chameleon", "config-example.yml");
            String chameleonConfigPath = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
            RuntimeExecUtils.rename(chameleonConfigOldPath, chameleonConfigPath);
            initConfigChangeParamsMap();
            setAllParams(workspaceId);
            changeAllConfig();
            deleteParamsConifg();
            writeChameleonOverrideType();
            copyConfigFiles(workspaceId);
        } catch (PortalException e) {
            LOGGER.error(e.toString());
            PortalControl.shutDownPortal(e.toString());
            return false;
        }
        String chameleonVenv = PropertitesUtils.getSinglePropertiesParameter(Chameleon.VENV_PATH,
                PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DROP, chameleonParameterTable, new ArrayList<>());
        useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.CREATE, chameleonParameterTable,
                new ArrayList<>());
        chameleonParameterTable.put("--source", "mysql");
        useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.ADD, chameleonParameterTable, new ArrayList<>());
        startChameleonReplicaOrder(chameleonVenv, Chameleon.Order.INIT, chameleonParameterTable,
                new ArrayList<>());
        if (PortalControl.status != Status.ERROR) {
            LOGGER.info("Mysql full migration is running.");
            PortalControl.status = Status.RUNNING_FULL_MIGRATION;
            return false;
        }
        return true;
    }

    /**
     *
     * initConfigChangeParamsMap
     */
    @Override
    public void initConfigChangeParamsMap() {
        configMap = new HashMap<>();
        configYmlChangeParamsMap.put(Chameleon.CONFIG_PATH, configMap);
    }

    /**
     *
     * initParmasFromEnvForAddAndChange
     */
    @Override
    public void initParmasFromEnvForAddAndChange() {
        configMap.putAll(YmlUtils.getChangeToolsYmlParameters(ToolsConfigEnum.CHAMELEON_CONFIG));
    }

    /**
     *
     * initParmasFromEnvForDelete
     */
    @Override
    public void initParmasFromEnvForDelete() {
        String deleteKeys = System.getProperty(ToolsConfigEnum.CHAMELEON_CONFIG.getConfigName());
        if (Strings.isNotBlank(deleteKeys)) {
            configDeleteParamsMap.put(Chameleon.CONFIG_PATH, List.of(deleteKeys.split(",")));
        }
    }

    /**
     *
     * initKafkaParams
     */
    @Override
    void initKafkaParams() {

    }

    /**
     * start
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean start(String workspaceId) {
        String chameleonVenv = PropertitesUtils.getSinglePropertiesParameter(Chameleon.VENV_PATH,
                PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        chameleonParameterTable.put("--source", "mysql");
        checkStatus(Chameleon.Order.INIT);
        if (PortalControl.toolsMigrationParametersTable.get(MigrationParameters.SNAPSHOT_OBJECT).equals("yes")) {
            useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_TRIGGER, chameleonParameterTable,
                    new ArrayList<>());
            useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_VIEW, chameleonParameterTable,
                    new ArrayList<>());
            useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_FUNC, chameleonParameterTable,
                    new ArrayList<>());
            useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_PROC, chameleonParameterTable,
                    new ArrayList<>());
        }
        chameleonParameterTable.clear();
        if (!PortalControl.taskList.contains(Command.Start.Mysql.INCREMENTAL) && shouldDetachReplica) {
            runDetach();
        }
        if (PortalControl.status != Status.ERROR) {
            LOGGER.info("Mysql full migration finished.");
            PortalControl.status = Status.FULL_MIGRATION_FINISHED;
        } else {
            LOGGER.error("Mysql full migration failed.");
        }
        return true;
    }

    /**
     * Clean data.
     *
     * @param workspaceId the workspace id
     */
    public void cleanData(String workspaceId) {
        String chameleonVenv = PropertitesUtils.getSinglePropertiesParameter(Chameleon.VENV_PATH,
                PortalControl.toolsConfigPath);
        String inputOrderPath = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        Hashtable<String, String> chameleonDropParameterTable = new Hashtable<>();
        chameleonDropParameterTable.put("--config", "default_" + workspaceId);
        useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DROP, chameleonDropParameterTable,
                new ArrayList<>());
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        ArrayList<String> fileList = new ArrayList<>();
        String chameleonOrderStr = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_";
        for (String order : Chameleon.Order.ALL_ORDER_LIST) {
            fileList.add(chameleonOrderStr + order + ".json");
        }
        fileList.add(inputOrderPath);
        try {
            for (String name : fileList) {
                RuntimeExecUtils.removeFile(name, PortalControl.portalErrorPath);
                ProcessUtils.sleepThread(100, "clean data");
            }
            FileUtils.createFile(inputOrderPath, true);
        } catch (PortalException e) {
            e.setRequestInformation("Clean data failed");
            LOGGER.error(e.toString());
        }
        ProcessUtils.sleepThread(100, "clean data");
    }

    /**
     * stop
     *
     * @return boolean
     */
    @Override
    public boolean stop() {
        return true;
    }

    /**
     * uninstall
     *
     * @return boolean
     */
    public boolean uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH));
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.PATH).replaceFirst("~",
                System.getProperty("user.home")));
        filePaths.add(PathUtils.combainPath(false, PortalControl.portalControlPath + "tmp", "chameleon"));
        InstallMigrationUtils.removeSingleMigrationToolFiles(filePaths, errorPath);
        return true;
    }

    /**
     * checkStatus
     *
     * @param order order
     * @return boolean
     */
    @Override
    public boolean checkStatus(String order) {
        if (Plan.stopPlan && !Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            return true;
        }
        String endFlag = order + " finished";
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        while (!Plan.stopPlan || Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            ProcessUtils.sleepThread(1000, "starting task");
            String processString = "chameleon " + order + " --config default_" + Plan.workspaceId;
            LOGGER.info(order + " running");
            boolean processQuit = ProcessUtils.getCommandPid(processString) == -1;
            boolean finished = LogViewUtils.lastLine(logPath).contains(endFlag);
            if (processQuit && finished) {
                LOGGER.info(order + " finished");
                break;
            } else if (processQuit) {
                String errMsg = LogViewUtils.getErrorMsg(logPath);
                PortalException e = new PortalException("Process " + processString + " exit abnormally", "checking "
                        + "chameleon replica order", errMsg);
                e.setRequestInformation("Run chameleon order " + order + " failed");
                e.setRepairTips("read " + logPath + " or error.log to get detailed information");
                LOGGER.error(e.toString());
                PortalControl.shutDownPortal(e.toString());
                return false;
            }
        }
        return true;
    }

    /**
     * reportProgress
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean reportProgress(String workspaceId) {
        ChangeStatusTools.changeFullStatus();
        return true;
    }

    /**
     * Run detach.
     */
    public void runDetach() {
        String chameleonVenv = PropertitesUtils.getSinglePropertiesParameter(Chameleon.VENV_PATH,
                PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + Plan.workspaceId);
        chameleonParameterTable.put("--source", "mysql");
        ArrayList<String> outputList = new ArrayList<>();
        outputList.add("YES");
        useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DETACH, chameleonParameterTable, outputList);
        shouldDetachReplica = false;
    }


    /**
     * Use chameleon replica order.
     *
     * @param chameleonVenvPath the chameleon venv path
     * @param order             the order
     * @param parametersTable   the parameters table
     * @param orderList         the order list
     */
    private void useChameleonReplicaOrder(String chameleonVenvPath, String
            order, Hashtable<String, String> parametersTable, ArrayList<String> orderList) {
        startChameleonReplicaOrder(chameleonVenvPath, order, parametersTable, orderList);
        checkStatus(order);
    }

    /**
     * Start chameleon replica order.
     *
     * @param chameleonVenvPath the chameleon venv path
     * @param order             the order
     * @param parametersTable   the parameters table
     * @param orderList         the order list
     */
    private void startChameleonReplicaOrder(String chameleonVenvPath, String
            order, Hashtable<String, String> parametersTable, ArrayList<String> orderList) {
        if (Plan.stopPlan && !Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            return;
        }
        String chameleonOrder = jointChameleonOrders(parametersTable, order);
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        try {
            RuntimeExecUtils.executeOrder(chameleonOrder, 2000, chameleonVenvPath, logPath, true, orderList);
        } catch (PortalException e) {
            e.setRequestInformation("Start chameleon order " + order + " failed");
            LOGGER.error(e.toString());
            PortalControl.shutDownPortal(e.toString());
        }
    }

    /**
     * Joint chameleon orders string.
     *
     * @param chameleonParameterTable the chameleon parameter table
     * @param order                   the order
     * @return the string
     */
    private static String jointChameleonOrders(Hashtable<String, String> chameleonParameterTable, String order) {
        String chameleonFile = PortalControl.toolsConfigParametersTable.get(Chameleon.RUNNABLE_FILE_PATH);
        StringBuilder chameleonOrder = new StringBuilder(chameleonFile + " " + order + " ");
        for (String key : chameleonParameterTable.keySet()) {
            chameleonOrder.append(key).append(" ").append(chameleonParameterTable.get(key)).append(" ");
        }
        return chameleonOrder.substring(0, chameleonOrder.length() - 1);
    }
}
