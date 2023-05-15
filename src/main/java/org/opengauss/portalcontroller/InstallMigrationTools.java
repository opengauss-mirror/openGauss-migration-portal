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
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.InstallWay;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class InstallMigrationTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallMigrationTools.class);
    private static final Map<String, CheckTask> MIGRATION_SERVICES = new HashMap<>();

    static {
        MIGRATION_SERVICES.put(MigrationParameters.Type.FULL, new CheckTaskMysqlFullMigration());
        MIGRATION_SERVICES.put(MigrationParameters.Type.INCREMENTAL, new CheckTaskIncrementalMigration());
        MIGRATION_SERVICES.put(MigrationParameters.Type.REVERSE, new CheckTaskReverseMigration());
        MIGRATION_SERVICES.put(MigrationParameters.Type.CHECK, new CheckTaskIncrementalDatacheck());
    }

    private static final LinkedHashMap<String, String> INSTALL_WAY_PARAMETER_HASH_MAP = new LinkedHashMap<>() {{
        put(Command.Install.Mysql.FullMigration.DEFAULT, MigrationParameters.Install.FULL_MIGRATION);
        put(Command.Install.Mysql.IncrementalMigration.DEFAULT, MigrationParameters.Install.INCREMENTAL_MIGRATION);
        put(Command.Install.Mysql.ReverseMigration.DEFAULT, MigrationParameters.Install.REVERSE_MIGRATION);
        put(Command.Install.Mysql.Check.DEFAULT, MigrationParameters.Install.DATACHECK);
    }};

    private static final LinkedHashMap<String, String[]> INSTALL_ORDER_LIST = new LinkedHashMap<>() {{
        put(Command.Install.Mysql.All.ONLINE, new String[]{
                Command.Install.Mysql.FullMigration.ONLINE, Command.Install.Mysql.IncrementalMigration.ONLINE, Command.Install.Mysql.ReverseMigration.ONLINE, Command.Install.Mysql.Check.ONLINE,
        });
        put(Command.Install.Mysql.All.OFFLINE, new String[]{
                Command.Install.Mysql.FullMigration.OFFLINE, Command.Install.Mysql.IncrementalMigration.OFFLINE, Command.Install.Mysql.ReverseMigration.OFFLINE, Command.Install.Mysql.Check.OFFLINE,
        });
        put(Command.Install.Mysql.All.DEFAULT, new String[]{
                Command.Install.Mysql.FullMigration.DEFAULT, Command.Install.Mysql.IncrementalMigration.DEFAULT, Command.Install.Mysql.ReverseMigration.DEFAULT, Command.Install.Mysql.Check.DEFAULT,
        });
    }};

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
        if (download) {
            RuntimeExecTools.download(pkgUrl, pkgPath);
        }
        Tools.installPackage(criticalFileList, pkgPath, pkgName, PortalControl.toolsConfigParametersTable.get(installPath));
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
            LOGGER.error(e.toString());
        }
    }

    /**
     * Uninstall migration tools.
     */
    public void uninstallAllMigrationTools() {
        for (CheckTask checkTask : MIGRATION_SERVICES.values()) {
            checkTask.uninstall();
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
    public boolean getInstallWay(String order) {
        if (Tools.containString(order, InstallWay.OFFLINE.getName())) return false;
        if (Tools.containString(order, InstallWay.ONLINE.getName())) return true;
        String installWayValue = PortalControl.toolsMigrationParametersTable.get(INSTALL_WAY_PARAMETER_HASH_MAP.get(order));
        return InstallWay.ONLINE.getName().equals(installWayValue);
    }

    /**
     * Gets check task.
     *
     * @param order the order
     * @return the check task
     */
    public CheckTask getCheckTask(String order) {
        String taskCommand = Arrays.stream(order.split(" ")).filter(part -> MigrationParameters.Type.ALL.contains(part)).findAny().get();
        return MIGRATION_SERVICES.getOrDefault(taskCommand, null);
    }

    /**
     * Run install order boolean.
     *
     * @param order the order
     * @return the boolean
     */
    public boolean runInstallOrder(String order) {
        try {
            getCheckTask(order).installAllPackages(getInstallWay(order));
        } catch (PortalException e) {
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
            return false;
        }
        return true;
    }

    /**
     * Run all install order.
     *
     * @param order the order
     */
    public void runAllInstallOrder(String order) {
        for (String singleOrder : INSTALL_ORDER_LIST.get(order)) {
            if (!runInstallOrder(singleOrder)) {
                return;
            }
        }
        Tools.outputResult(true, Parameter.INSTALL_ALL_MIGRATION_TOOLS);
    }
}