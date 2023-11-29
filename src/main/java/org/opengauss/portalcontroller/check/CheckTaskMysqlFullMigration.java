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

import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.*;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import static org.opengauss.portalcontroller.Plan.runningTaskList;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;

/**
 * The type Check task mysql full migration.
 */
public class CheckTaskMysqlFullMigration implements CheckTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskMysqlFullMigration.class);

    /**
     * The constant shouldDetachReplica.
     */
    public static boolean shouldDetachReplica = true;

    public void installAllPackages(boolean download) throws PortalException {
        if (download) {
            RuntimeExecTools.download(Chameleon.PKG_URL, Chameleon.PKG_PATH);
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String chameleonInstallPath = hashtable.get(Chameleon.INSTALL_PATH);
        String chameleonVersion = "chameleon-" + hashtable.get(Parameter.TOOL_VERSION);
        String chameleonInstallLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath
                + "tools", "chameleon", chameleonVersion, "install_chameleon.log");
        Tools.createFile(chameleonInstallPath, false);
        String chameleonVersionOrder = hashtable.get(Chameleon.RUNNABLE_FILE_PATH) + " --version";
        if (checkChameleonStatus(chameleonVersionOrder, chameleonInstallLogPath)) {
            LOGGER.info("check chameleon success...");
            return;
        }
        LOGGER.error("first check chameleon failed, start install...");
        String chameleonPkgSpace = "200MB";
        String chameleonPkgPath = hashtable.get(Chameleon.PKG_PATH) + hashtable.get(Chameleon.PKG_NAME);
        RuntimeExecTools.unzipFile(chameleonPkgPath, chameleonPkgSpace, chameleonInstallPath);
        String buildChameleonName = "install.sh";
        String chameleonVenvPath = hashtable.get(Chameleon.VENV_PATH);
        RuntimeExecTools.runShell(buildChameleonName, chameleonVenvPath);
        checkFileExist(hashtable.get(Chameleon.RUNNABLE_FILE_PATH), 300);
        checkChameleonVersion(chameleonVersionOrder, chameleonInstallLogPath);
    }

    private static void checkFileExist(String filePath, int timeout) {
        int timeOutCount = 0;
        while (!(new File(filePath).exists()) && timeOutCount < timeout) {
            try {
                TimeUnit.SECONDS.sleep(1);
                LOGGER.info("check file exist sleep : {} s", timeOutCount);
            } catch (InterruptedException e) {
                LOGGER.error("sleep exception:", e);
            }
            timeOutCount++;
        }
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
            RuntimeExecTools.executeOrder(order, 3000, PortalControl.portalControlPath,
                    chameleonTestLogPath, true, new ArrayList<>());
        } catch (PortalException portalException) {
            String logStr = Tools.outputFileString(chameleonInstallLogPath);
            if (logStr.equals("")) {
                portalException.setRequestInformation("Please check pip download source.");
            } else {
                portalException.setRequestInformation(logStr);
            }
            throw portalException;
        }
        String log = LogView.getFullLog(chameleonTestLogPath).trim();
        if (log.startsWith("chameleon")) {
            LOGGER.info("Install chameleon success.");
        } else {
            throw new PortalException("Portal exception", "installing chameleon",
                    "Install chameleon failed.Information:" + log);
        }
        RuntimeExecTools.removeFile(chameleonTestLogPath, PortalControl.portalErrorPath);
    }

    /**
     * first Check chameleon version.
     *
     * @param order                   the order
     * @param chameleonInstallLogPath the chameleon install log path
     * @return boolean
     *
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
            RuntimeExecTools.executeOrder(chameleonRunnableFilePath + " set_configuration_files", 3000, PortalControl.portalErrorPath);
        } catch (PortalException e) {
            e.setRequestInformation("Set configuration files failed");
            throw e;
        }
        try {
            String newFileName = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config", "chameleon", "default_" + workspaceId + ".yml");
            Tools.createFile(fileDirectory, false);
            RuntimeExecTools.copyFile(newFileName, fileDirectory, true);
        } catch (PortalException e) {
            e.setRequestInformation("Copy config files failed");
            throw e;
        }
    }

    public void changeParameters(String workspaceId) throws PortalException {
        try {
            String chameleonConfigOldPath = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config", "chameleon", "config-example.yml");
            String chameleonConfigPath = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
            RuntimeExecTools.rename(chameleonConfigOldPath, chameleonConfigPath);
            Tools.createFile(PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "pid"), false);
            HashMap<String, Object> hashMap = new HashMap<>();
            hashMap.put("pid_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "pid"));
            hashMap.put("sources.mysql.csv_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "tmp"));
            hashMap.put("sources.mysql.out_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "tmp"));
            hashMap.put("dump_json", "yes");
            if (Strings.isNotBlank(toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL))) {
                hashMap.put("log_level",
                        toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL).toLowerCase());
            }
            Tools.changeYmlParameters(hashMap, chameleonConfigPath);
            Tools.changeFullMigrationParameters(PortalControl.toolsMigrationParametersTable);
        } catch (PortalException e) {
            e.setRequestInformation("Create folder failed");
            e.setRepairTips("ensure the config folder " + PortalControl.portalWorkSpacePath + " is available");
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
            throw e;
        }
    }

    @Override
    public void prepareWork(String workspaceId) {
        runningTaskList.add(Command.Start.Mysql.FULL);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION;
        }
        try {
            changeParameters(workspaceId);
            copyConfigFiles(workspaceId);
            Tools.changeToolsYmlParameters(ToolsConfigEnum.CHAMELEON_CONFIG,
                    PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH));
            Tools.deleteParams(ToolsConfigEnum.CHAMELEON_CONFIG.getConfigName(),
                    PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH));
        } catch (PortalException e) {
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
            return;
        }
        Task task = new Task();
        String chameleonVenv = Tools.getSinglePropertiesParameter(Chameleon.VENV_PATH, PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DROP, chameleonParameterTable, new ArrayList<>());
        task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.CREATE, chameleonParameterTable, new ArrayList<>());
        chameleonParameterTable.put("--source", "mysql");
        task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.ADD, chameleonParameterTable, new ArrayList<>());
        task.startChameleonReplicaOrder(chameleonVenv, Chameleon.Order.INIT, chameleonParameterTable, new ArrayList<>());
        if (PortalControl.status != Status.ERROR) {
            LOGGER.info("Mysql full migration is running.");
            PortalControl.status = Status.RUNNING_FULL_MIGRATION;
        }
    }

    @Override
    public void start(String workspaceId) {
        Task task = new Task();
        String chameleonVenv = Tools.getSinglePropertiesParameter(Chameleon.VENV_PATH, PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        chameleonParameterTable.put("--source", "mysql");
        task.checkChameleonReplicaOrder(Chameleon.Order.INIT);
        if (PortalControl.toolsMigrationParametersTable.get(MigrationParameters.SNAPSHOT_OBJECT).equals("yes")) {
            task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_TRIGGER, chameleonParameterTable, new ArrayList<>());
            task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_VIEW, chameleonParameterTable, new ArrayList<>());
            task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_FUNC, chameleonParameterTable, new ArrayList<>());
            task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.START_PROC, chameleonParameterTable, new ArrayList<>());
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
    }

    /**
     * Clean data.
     *
     * @param workspaceId the workspace id
     */
    public void cleanData(String workspaceId) {
        Task task = new Task();
        String chameleonVenv = Tools.getSinglePropertiesParameter(Chameleon.VENV_PATH, PortalControl.toolsConfigPath);
        String inputOrderPath = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        Hashtable<String, String> chameleonDropParameterTable = new Hashtable<>();
        chameleonDropParameterTable.put("--config", "default_" + workspaceId);
        task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DROP, chameleonDropParameterTable, new ArrayList<>());
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        ArrayList<String> fileList = new ArrayList<>();
        String chameleonOrderStr = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_";
        for (String order : Chameleon.Order.ALL_ORDER_LIST) {
            fileList.add(chameleonOrderStr + order + ".json");
        }
        fileList.add(inputOrderPath);
        try {
            for (String name : fileList) {
                RuntimeExecTools.removeFile(name, PortalControl.portalErrorPath);
                Tools.sleepThread(100, "clean data");
            }
            Tools.createFile(inputOrderPath, true);
        } catch (PortalException e) {
            e.setRequestInformation("Clean data failed");
            LOGGER.error(e.toString());
        }
        Tools.sleepThread(100, "clean data");
    }

    public void checkEnd() {

    }

    public void uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH));
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.PATH).replaceFirst("~", System.getProperty("user.home")));
        filePaths.add(PathUtils.combainPath(false, PortalControl.portalControlPath + "tmp", "chameleon"));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }

    /**
     * Run detach.
     */
    public static void runDetach() {
        Task task = new Task();
        String chameleonVenv = Tools.getSinglePropertiesParameter(Chameleon.VENV_PATH, PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + Plan.workspaceId);
        chameleonParameterTable.put("--source", "mysql");
        ArrayList<String> outputList = new ArrayList<>();
        outputList.add("YES");
        task.useChameleonReplicaOrder(chameleonVenv, Chameleon.Order.DETACH, chameleonParameterTable, outputList);
        shouldDetachReplica = false;
    }

}
