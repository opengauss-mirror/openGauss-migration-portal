package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.*;
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

    public boolean installAllPackages(boolean download) {
        if (download) {
            RuntimeExecTools.download(Chameleon.PKG_URL, Chameleon.PKG_PATH);
        }
        boolean flag = true;
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String chameleonVenvPath = hashtable.get(Chameleon.VENV_PATH);
        String chameleonPkgPath = Tools.getPackagePath(Chameleon.PKG_PATH, Chameleon.PKG_NAME);
        String chameleonRunnableFilePath = hashtable.get(Chameleon.RUNNABLE_FILE_PATH);
        String chameleonInstallLogPath = PortalControl.portalControlPath + "logs" + File.separator + "install_chameleon.log";
        String chameleonTestLogPath = PortalControl.portalControlPath + "logs" + File.separator + "test_chameleon.log";
        Tools.createFile(chameleonVenvPath, false);
        RuntimeExecTools.executeOrder("python3 -m venv " + chameleonVenvPath + "venv", 3000, PortalControl.portalErrorPath);
        String installCommand = chameleonVenvPath + "venv" + File.separator + "bin" + File.separator + "pip3 install " + chameleonPkgPath;
        RuntimeExecTools.executeOrder(installCommand, 3000, PortalControl.portalControlPath, chameleonInstallLogPath, true);
        File chameleonFile = new File(chameleonRunnableFilePath);
        if (chameleonFile.exists()) {
            LOGGER.info("Chameleon has been installed.If you want to update the chameleon.Please uninstall the chameleon first.");
        } else {
            LOGGER.info("Installing chameleon...");
            while (true) {
                Tools.sleepThread(1000, "waiting for process running");
                if (Tools.getCommandPid(installCommand) == -1) {
                    String chameleonVersion = chameleonRunnableFilePath + " --version";
                    RuntimeExecTools.executeOrder(chameleonVersion, 3000, PortalControl.portalControlPath, chameleonTestLogPath, true);
                    if (Tools.readFile(new File(chameleonTestLogPath)).equals("")) {
                        flag = false;
                        LOGGER.error("Error message: Install chameleon failed.");
                        if (Tools.outputFileString(chameleonInstallLogPath).equals("")) {
                            LOGGER.warn("Please check pip download source.");
                            LOGGER.warn("Or you can try to set pip download source to http://mirrors.aliyun.com/pypi/simple/");
                        }
                    } else {
                        LOGGER.info("Install chameleon success.");
                    }
                    RuntimeExecTools.removeFile(chameleonTestLogPath, PortalControl.portalErrorPath);
                    break;
                }
            }
        }
        return flag;
    }


    /**
     * Install chameleon package.
     */
    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskMysqlFullMigration();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.FULL_MIGRATION);
        return flag;
    }

    /**
     * Copy chameleon files.
     */
    @Override
    public void copyConfigFiles(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String chameleonRunnableFilePath = hashtable.get(Chameleon.RUNNABLE_FILE_PATH);
        RuntimeExecTools.executeOrder(chameleonRunnableFilePath + " set_configuration_files", 3000, PortalControl.portalErrorPath);
        String chameleonPath = hashtable.get(Chameleon.PATH).replaceFirst("~", System.getProperty("user.home"));
        String fileDirectory = chameleonPath + "configuration" + File.separator;
        String newFileName = PortalControl.portalWorkSpacePath + "config" + File.separator + "chameleon" + File.separator + "default_" + workspaceId + ".yml";
        Tools.createFile(fileDirectory, false);
        RuntimeExecTools.copyFile(newFileName, fileDirectory, true);
    }

    /**
     * Change chameleon parameters.
     */
    @Override
    public void changeParameters(String workspaceId) {
        String chameleonConfigOldPath = PortalControl.portalWorkSpacePath + "config" + File.separator + "chameleon" + File.separator + "config-example.yml";
        String chameleonConfigPath = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
        RuntimeExecTools.rename(chameleonConfigOldPath, chameleonConfigPath);
        Tools.createFile(PortalControl.portalWorkSpacePath + "pid" + File.separator, false);
        Tools.changeSingleYmlParameter("pid_dir", PortalControl.portalWorkSpacePath + "pid" + File.separator, chameleonConfigPath);
        Tools.changeSingleYmlParameter("sources.mysql.out_dir", PortalControl.portalWorkSpacePath + "tmp", chameleonConfigPath);
        Tools.changeSingleYmlParameter("dump_json", "yes", chameleonConfigPath);
        Tools.changeFullMigrationParameters(PortalControl.toolsMigrationParametersTable);
    }

    @Override
    public void prepareWork(String workspaceId) {
        runningTaskList.add(Command.Start.Mysql.FULL);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION;
        }
        changeParameters(workspaceId);
        copyConfigFiles(workspaceId);
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
        for (String name : fileList) {
            RuntimeExecTools.removeFile(name, PortalControl.portalErrorPath);
            Tools.sleepThread(100, "clean data");
        }
        Tools.createFile(inputOrderPath, true);
        Tools.sleepThread(100, "clean data");
    }

    public void checkEnd() {

    }

    public void uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH) + "venv");
        filePaths.add(PortalControl.toolsConfigParametersTable.get(Chameleon.PATH).replaceFirst("~", System.getProperty("user.home")));
        filePaths.add(PortalControl.portalControlPath + "tmp" + File.separator + "chameleon");
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }

}
