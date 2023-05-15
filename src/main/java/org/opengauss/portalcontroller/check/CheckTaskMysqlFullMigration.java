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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.Plan.runningTaskList;

/**
 * The type Check task mysql full migration.
 */
public class CheckTaskMysqlFullMigration implements CheckTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskMysqlFullMigration.class);

    public void installAllPackages(boolean download) throws PortalException {
        if (download) {
            RuntimeExecTools.download(Chameleon.PKG_URL, Chameleon.PKG_PATH);
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String chameleonVenvPath = hashtable.get(Chameleon.VENV_PATH);
        String chameleonPkgPath = Tools.getPackagePath(Chameleon.PKG_PATH, Chameleon.PKG_NAME);
        String chameleonRunnableFilePath = hashtable.get(Chameleon.RUNNABLE_FILE_PATH);
        String chameleonInstallLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "logs", "install_chameleon.log");
        String chameleonTestLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "logs", "test_chameleon.log");
        String createVenvCommand = "python3 -m venv " + chameleonVenvPath + "venv";
        String installExeucteFile = PathUtils.combainPath(true, chameleonVenvPath + "venv", "bin", "pip3");
        String installCommand = installExeucteFile + " install " + chameleonPkgPath;
        try {
            Tools.createFile(chameleonVenvPath, false);
            RuntimeExecTools.executeOrder(createVenvCommand, 3000, PortalControl.portalErrorPath);
            LOGGER.info("Installing chameleon ...");
            RuntimeExecTools.executeOrder(installCommand, 3000, PortalControl.portalControlPath, chameleonInstallLogPath, true);
        } catch (PortalException e) {
            e.setRequestInformation("Install package failed");
            throw e;
        }
        while (true) {
            Tools.sleepThread(1000, "waiting for process running");
            if (Tools.getCommandPid(installCommand) == -1) {
                String chameleonVersion = chameleonRunnableFilePath + " --version";
                RuntimeExecTools.executeOrder(chameleonVersion, 3000, PortalControl.portalControlPath, chameleonTestLogPath, true);
                if (Tools.readFile(new File(chameleonTestLogPath)).contains("chameleon")) {
                    LOGGER.info("Install chameleon success.");
                } else {
                    PortalException portalException = new PortalException("Portal exception", "installing chameleon", "Install chameleon failed.");
                    if (Tools.outputFileString(chameleonInstallLogPath).equals("")) {
                        portalException.setRequestInformation("Please check pip download source.");
                    }
                    throw portalException;
                }
                RuntimeExecTools.removeFile(chameleonTestLogPath, PortalControl.portalErrorPath);
                break;
            }
        }
    }

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
            Tools.changeSingleYmlParameter("pid_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "pid"), chameleonConfigPath);
            Tools.changeSingleYmlParameter("sources.mysql.out_dir", PathUtils.combainPath(false, PortalControl.portalWorkSpacePath + "tmp"), chameleonConfigPath);
            Tools.changeSingleYmlParameter("dump_json", "yes", chameleonConfigPath);
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
        } catch (PortalException e) {
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
            return;
        }
        Task task = new Task();
        String chameleonVenv = Tools.getSinglePropertiesParameter(Chameleon.VENV_PATH, PortalControl.toolsConfigPath);
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        task.useChameleonReplicaOrder(chameleonVenv, "drop_replica_schema", chameleonParameterTable);
        task.useChameleonReplicaOrder(chameleonVenv, "create_replica_schema", chameleonParameterTable);
        chameleonParameterTable.put("--source", "mysql");
        task.useChameleonReplicaOrder(chameleonVenv, "add_source", chameleonParameterTable);
        task.startChameleonReplicaOrder(chameleonVenv, "init_replica", chameleonParameterTable);
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
        task.checkChameleonReplicaOrder("init_replica");
        if (PortalControl.toolsMigrationParametersTable.get(MigrationParameters.SNAPSHOT_OBJECT).equals("yes")) {
            task.useChameleonReplicaOrder(chameleonVenv, "start_trigger_replica", chameleonParameterTable);
            task.useChameleonReplicaOrder(chameleonVenv, "start_view_replica", chameleonParameterTable);
            task.useChameleonReplicaOrder(chameleonVenv, "start_func_replica", chameleonParameterTable);
            task.useChameleonReplicaOrder(chameleonVenv, "start_proc_replica", chameleonParameterTable);
        }
        chameleonParameterTable.clear();
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
        Hashtable<String, String> chameleonParameterTable = new Hashtable<>();
        chameleonParameterTable.put("--config", "default_" + workspaceId);
        task.useChameleonReplicaOrder(chameleonVenv, "drop_replica_schema", chameleonParameterTable);
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        ArrayList<String> fileList = new ArrayList<>();
        String chameleonOrderStr = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_";
        fileList.add(chameleonOrderStr + "drop_replica_schema.json");
        fileList.add(chameleonOrderStr + "create_replica_schema.json");
        fileList.add(chameleonOrderStr + "add_source.json");
        fileList.add(chameleonOrderStr + "init_replica.json");
        fileList.add(chameleonOrderStr + "start_view_replica.json");
        fileList.add(chameleonOrderStr + "start_trigger_replica.json");
        fileList.add(chameleonOrderStr + "start_proc_replica.json");
        fileList.add(chameleonOrderStr + "start_func_replica.json");
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
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH) + "venv");
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.PATH).replaceFirst("~", System.getProperty("user.home")));
        filePaths.add(PathUtils.combainPath(false, PortalControl.portalControlPath + "tmp", "chameleon"));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }

}
