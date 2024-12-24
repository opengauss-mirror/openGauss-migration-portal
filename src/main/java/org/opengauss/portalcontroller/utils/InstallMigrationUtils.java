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

package org.opengauss.portalcontroller.utils;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.enums.InstallWay;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Software;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.MysqlFullMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Install migration tools.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class InstallMigrationUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallMigrationUtils.class);
    private static final Map<String, Tool> MIGRATION_SERVICES = new HashMap<>();
    private static boolean hasSudoPermission = false;

    static {
        MIGRATION_SERVICES.put(MigrationParameters.Type.FULL, new MysqlFullMigrationTool());
        MIGRATION_SERVICES.put(MigrationParameters.Type.INCREMENTAL, new IncrementalMigrationTool());
        MIGRATION_SERVICES.put(MigrationParameters.Type.REVERSE, new ReverseMigrationTool());
        MIGRATION_SERVICES.put(MigrationParameters.Type.CHECK, new IncrementalDatacheckTool());
    }

    private static final LinkedHashMap<String, String> INSTALL_WAY_PARAMETER_HASH_MAP = new LinkedHashMap<>() {{
        put(Command.Install.Mysql.FullMigration.DEFAULT, MigrationParameters.Install.FULL_MIGRATION);
        put(Command.Install.Mysql.IncrementalMigration.DEFAULT, MigrationParameters.Install.INCREMENTAL_MIGRATION);
        put(Command.Install.Mysql.ReverseMigration.DEFAULT, MigrationParameters.Install.REVERSE_MIGRATION);
        put(Command.Install.Mysql.Check.DEFAULT, MigrationParameters.Install.DATACHECK);
    }};

    private static final LinkedHashMap<String, String[]> INSTALL_ORDER_LIST = new LinkedHashMap<>() {{
        put(Command.Install.Mysql.All.ONLINE, new String[]{
                Command.Install.Mysql.FullMigration.ONLINE, Command.Install.Mysql.IncrementalMigration.ONLINE,
                Command.Install.Mysql.ReverseMigration.ONLINE, Command.Install.Mysql.Check.ONLINE,
        });
        put(Command.Install.Mysql.All.OFFLINE, new String[]{
                Command.Install.Mysql.FullMigration.OFFLINE, Command.Install.Mysql.IncrementalMigration.OFFLINE,
                Command.Install.Mysql.ReverseMigration.OFFLINE, Command.Install.Mysql.Check.OFFLINE,
        });
        put(Command.Install.Mysql.All.DEFAULT, new String[]{
                Command.Install.Mysql.FullMigration.DEFAULT, Command.Install.Mysql.IncrementalMigration.DEFAULT,
                Command.Install.Mysql.ReverseMigration.DEFAULT, Command.Install.Mysql.Check.DEFAULT,
        });
    }};

    public static boolean hasSudoPermission() {
        return hasSudoPermission;
    }

    /**
     * Install package boolean.
     *
     * @param filePathList     the file path list
     * @param pkgPathParameter the pkg path parameter
     * @param pkgNameParameter the pkg name parameter
     * @param pkgSpace         the pkg space
     * @param installPath      the install path
     * @throws PortalException the portal exception
     */
    public static void installPackage(ArrayList<String> filePathList, String pkgPathParameter,
                                      String pkgNameParameter, String pkgSpace,
                                      String installPath) throws PortalException {
        String packagePath = PathUtils.getPackagePath(pkgPathParameter, pkgNameParameter);
        FileUtils.createFile(installPath, false);
        RuntimeExecUtils.unzipFile(packagePath, pkgSpace, installPath);
        for (String path : filePathList) {
            File file = new File(path);
            if (!file.exists()) {
                throw new PortalException("Portal exception", "installing package " + packagePath,
                        "Install package " + packagePath + " to " + path + " failed");
            }
        }
        LogViewUtils.outputResult(true, "Install package " + packagePath);
    }

    /**
     * Install single migration tool boolean.
     *
     * @param software the software
     * @param download the download
     * @throws PortalException the portal exception
     */
    public void installSingleMigrationSoftware(Software software, boolean download) throws PortalException {
        ArrayList<String> criticalFileList = software.initCriticalFileList();
        Hashtable<String, String> initParameterHashtable = software.initParameterHashtable();
        String installPath = initParameterHashtable.get(Parameter.INSTALL_PATH);
        String pkgName = initParameterHashtable.get(Parameter.PKG_NAME);
        String pkgUrl = initParameterHashtable.get(Parameter.PKG_URL);
        String pkgPath = initParameterHashtable.get(Parameter.PKG_PATH);
        String pkgSpace = initParameterHashtable.get(Parameter.PKG_UNZIP_SPACE);
        if (download) {
            RuntimeExecUtils.download(pkgUrl, pkgPath);
        }
        installPackage(criticalFileList, pkgPath, pkgName, pkgSpace,
                PortalControl.toolsConfigParametersTable.get(installPath));
    }

    /**
     * Remove single migration tool files.
     *
     * @param filePaths the file paths
     * @param errorPath the error path
     */
    public static void removeSingleMigrationToolFiles(ArrayList<String> filePaths, String errorPath) {
        try {
            for (String path : filePaths) {
                RuntimeExecUtils.removeFile(path, errorPath);
            }
        } catch (PortalException e) {
            e.setRequestInformation("Cannot remove files.Uninstall migration tool failed");
            LOGGER.error(e.toString());
        }
    }

    /**
     * Uninstall migration tools.
     */
    public void uninstallAllMigrationTools() {
        for (Tool tool : MIGRATION_SERVICES.values()) {
            tool.uninstall();
        }
    }

    /**
     * Uninstall migration tools.
     *
     * @param order the order
     */
    public void uninstallMigrationTools(String order) {
        getCheckTask(order).uninstall();
    }

    /**
     * Gets install way.
     *
     * @param order the order
     * @return the install way
     */
    public static boolean getInstallWay(String order) {
        if (CommandUtils.containString(order, InstallWay.OFFLINE.getName())) return false;
        if (CommandUtils.containString(order, InstallWay.ONLINE.getName())) return true;
        String installWayValue =
                PortalControl.toolsMigrationParametersTable.get(INSTALL_WAY_PARAMETER_HASH_MAP.get(order));
        return InstallWay.ONLINE.getName().equals(installWayValue);
    }

    /**
     * Gets check task.
     *
     * @param order the order
     * @return the check task
     */
    public static Tool getCheckTask(String order) {
        String taskCommand =
                Arrays.stream(order.split(" ")).filter(part -> MigrationParameters.Type.ALL.contains(part)).findAny().get();
        return MIGRATION_SERVICES.getOrDefault(taskCommand, null);
    }

    /**
     * Run install order boolean.
     *
     * @param order the order
     * @return the boolean
     */
    public static boolean runInstallOrder(String order) {
        if (!getCheckTask(order).install(getInstallWay(order))) {
            PortalControl.shutDownPortal("install faied" + order);
            return false;
        }
        return true;
    }

    /**
     * Run all install order.
     *
     * @param order the order
     */
    public static void runAllInstallOrder(String order) {
        for (String singleOrder : INSTALL_ORDER_LIST.get(order)) {
            if (!runInstallOrder(singleOrder)) {
                return;
            }
        }
        LogViewUtils.outputResult(true, Parameter.INSTALL_ALL_MIGRATION_TOOLS);
    }

    /**
     * install dependencies
     *
     * @param scriptParam script param
     */
    public static void installDependencies(String scriptParam) {
        String installScript = "install_dependencies.sh";
        String workDirectory = PathUtils.combainPath(false, PortalControl.portalControlPath
                + "pkg", "dependencies");
        if (!new File(workDirectory + installScript).isFile()) {
            LOGGER.error("The " + workDirectory + installScript
                    + " does not point to a regular file or the file does not exist.");
            return;
        }

        LOGGER.info("Start to install " + scriptParam + " dependencies.");
        String command = String.format("sh %s %s", installScript, scriptParam);
        String logPath = PathUtils.combainPath(true, PortalControl.portalControlPath
                + "logs", "dependencies_install.log");
        try {
            RuntimeExecUtils.executeOrder(command, 3000, workDirectory, logPath, true, new ArrayList<>());
        } catch (PortalException e) {
            LOGGER.error(e.getMessage());
        }

        ProcessUtils.sleepThread(1000, "run shell: " + command);
        LOGGER.info("The dependencies of " + scriptParam + " installation is complete."
                + " Logs are recorded in " + logPath + ".");
    }

    /**
     * check sudo permission
     *
     * @return boolean
     */
    public static boolean checkSudoPermission() {
        String command = "sudo -n true";
        try {
            Process process = Runtime.getRuntime().exec(command);
            if (process.waitFor() == 0) {
                hasSudoPermission = true;
                return true;
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Error checking sudo permission. Error massage: {}", e.getMessage());
        }
        LOGGER.warn("The installation user does not have the sudo permission, or a password is required.");
        return false;
    }
}