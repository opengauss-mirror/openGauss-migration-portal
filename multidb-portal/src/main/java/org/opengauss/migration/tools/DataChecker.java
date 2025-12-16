/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.DataCheckerConstants;
import org.opengauss.exceptions.InstallException;
import org.opengauss.config.Portal;

/**
 * data-checker
 *
 * @since 2025/2/17
 */
@Getter
public class DataChecker extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(DataChecker.class);

    private static volatile DataChecker instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;

    private final String dataCheckerDirPath;
    private final String libDirPath;
    private final String checkJarPath;
    private final String extractJarPath;

    private DataChecker() {
        Portal portal = Portal.getInstance();
        String portalVersion = PortalConstants.PORTAL_VERSION;

        this.pkgDirPath = String.format("%s/%s", portal.getPortalPkgDirPath(),
                DataCheckerConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = String.format(DataCheckerConstants.INSTALL_PKG_NAME_MODEL, portalVersion);
        this.installDirPath = String.format("%s/%s", portal.getPortalToolsDirPath(),
                DataCheckerConstants.INSTALL_DIR_NAME);

        String dataCheckerDirName = String.format(DataCheckerConstants.DATA_CHECKER_HOME_DIR_NAME_MODEL, portalVersion);
        String checkJarName = String.format(DataCheckerConstants.CHECK_JAR_NAME_MODEL, portalVersion);
        String extractJarName = String.format(DataCheckerConstants.EXTRACT_JAR_NAME_MODEL, portalVersion);
        this.dataCheckerDirPath = String.format("%s/%s", this.installDirPath, dataCheckerDirName);
        this.libDirPath = String.format("%s/%s", dataCheckerDirPath, DataCheckerConstants.DATA_CHECKER_LIB_DIR_NAME);
        this.checkJarPath = String.format("%s/%s", dataCheckerDirPath, checkJarName);
        this.extractJarPath = String.format("%s/%s", dataCheckerDirPath, extractJarName);
    }

    /**
     * Get instance of DataChecker
     *
     * @return DataChecker instance
     */
    public static DataChecker getInstance() {
        if (instance == null) {
            synchronized (DataChecker.class) {
                if (instance == null) {
                    instance = new DataChecker();
                }
            }
        }
        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("DataChecker is already installed");
            return;
        }

        LOGGER.info("Start to install DataChecker");
        LOGGER.info("Create DataChecker install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Unzip DataChecker install package");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check DataChecker install files");
        checkKeyFileExists(checkJarPath);
        checkKeyFileExists(extractJarPath);
        LOGGER.info("Install DataChecker successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("DataChecker is not installed");
            return;
        }

        LOGGER.info("Uninstall DataChecker");
        deletePath(installDirPath);
        LOGGER.info("Uninstall DataChecker successfully");
    }

    @Override
    public String getToolName() {
        return DataCheckerConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(checkJarPath);
            checkKeyFileExists(extractJarPath);
        } catch (InstallException e) {
            return false;
        }
        return true;
    }
}
