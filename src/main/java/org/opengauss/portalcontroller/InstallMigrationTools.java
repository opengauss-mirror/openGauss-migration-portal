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
import org.opengauss.portalcontroller.constant.*;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Install migration tools.
 */
public class InstallMigrationTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallMigrationTools.class);

    /**
     * Install migration tools boolean.
     *
     * @param softwareArrayList the software array list
     * @param download          the download
     * @return the boolean
     */
    public static boolean installMigrationTools(ArrayList<Software> softwareArrayList, boolean download) {
        boolean flag = true;
        for (Software software : softwareArrayList) {
            flag = InstallMigrationTools.installSingleMigrationTool(software, download);
            if (!flag) {
                break;
            }
        }
        return flag;
    }

    /**
     * Install single migration tool boolean.
     *
     * @param software the software
     * @param download the download
     * @return the boolean
     */
    public static boolean installSingleMigrationTool(Software software, boolean download) {
        try {
            boolean flag = true;
            ArrayList<String> criticalFileList = software.initCriticalFileList();
            Hashtable<String, String> initParameterHashtable = software.initParameterHashtable();
            String installPath = initParameterHashtable.get(Parameter.INSTALL_PATH);
            String path = initParameterHashtable.get(Parameter.PATH);
            String pkgName = initParameterHashtable.get(Parameter.PKG_NAME);
            String pkgUrl = initParameterHashtable.get(Parameter.PKG_URL);
            String pkgPath = initParameterHashtable.get(Parameter.PKG_PATH);
            if (download) {
                flag = RuntimeExecTools.download(pkgUrl, pkgPath);
                Tools.outputResult(flag, "Download " + pkgUrl);
            }
            flag = Tools.installPackage(criticalFileList, pkgPath, pkgName, PortalControl.toolsConfigParametersTable.get(installPath), path);
            Tools.outputResult(flag, "Install " + PortalControl.toolsConfigParametersTable.get(pkgName));
            return flag;
        } catch (PortalException e) {
            e.shutDownPortal(LOGGER);
            return false;
        }
    }

    /**
     * Install single migration tool boolean.
     *
     * @param checkTask             the check task
     * @param installationParameter the installation parameter
     * @return the boolean
     */
    public static boolean installSingleMigrationTool(CheckTask checkTask, String installationParameter) {
        boolean flag = true;
        String installWay = PortalControl.toolsMigrationParametersTable.get(installationParameter);
        if (installWay.equals("online")) {
            flag = checkTask.installAllPackages(true);
        } else if (installWay.equals("offline")) {
            flag = checkTask.installAllPackages(false);
        } else {
            flag = false;
            LOGGER.error("Error message: Please check " + installationParameter + " in migrationConfig.properties.This property must be online or offline.");
        }
        return flag;
    }


    /**
     * Install all migration tools.
     *
     * @param checkTasks the check tasks
     */
    public static void installAllMigrationTools(ArrayList<CheckTask> checkTasks) {
        boolean flag = true;
        for (CheckTask checkTask : checkTasks) {
            flag = checkTask.installAllPackages();
            if (!flag) {
                break;
            }
        }
        Tools.outputResult(flag, Parameter.INSTALL_ALL_MIGRATION_TOOLS);
    }

    /**
     * Install all migration tools.
     *
     * @param download   the download
     * @param checkTasks the check tasks
     */
    public static void installAllMigrationTools(boolean download, ArrayList<CheckTask> checkTasks) {
        boolean flag = true;
        for (CheckTask checkTask : checkTasks) {
            flag = checkTask.installAllPackages(download);
            if (!flag) {
                break;
            }
        }
        Tools.outputResult(flag, Parameter.INSTALL_ALL_MIGRATION_TOOLS);
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
                RuntimeExecTools.removeFile(path, errorPath);
            }
        } catch (PortalException e) {
            e.setRequestInformation("Cannot remove files.Uninstall migration tool failed");
            e.printLog(LOGGER);
        }
    }

    /**
     * Uninstall migration tools.
     */
    public static void uninstallMigrationTools() {
        ArrayList<CheckTask> checkTaskList = new ArrayList<>();
        checkTaskList.add(new CheckTaskMysqlFullMigration());
        checkTaskList.add(new CheckTaskIncrementalMigration());
        checkTaskList.add(new CheckTaskIncrementalDatacheck());
        for (CheckTask checkTask : checkTaskList) {
            checkTask.uninstall();
        }
    }

}