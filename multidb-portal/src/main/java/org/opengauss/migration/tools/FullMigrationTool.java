/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.FullMigrationToolConstants;
import org.opengauss.exceptions.InstallException;

/**
 * full migration tool
 *
 * @since 2025/5/29
 */
@Getter
public class FullMigrationTool extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(FullMigrationTool.class);

    private static volatile FullMigrationTool instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;
    private final String jarPath;

    private FullMigrationTool() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();

        this.pkgDirPath = String.format("%s/%s", applicationConfig.getPortalPkgDirPath(),
                FullMigrationToolConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = String.format(FullMigrationToolConstants.INSTALL_PKG_NAME, PortalConstants.PORTAL_VERSION);
        this.installDirPath = String.format("%s/%s", applicationConfig.getPortalToolsDirPath(),
                FullMigrationToolConstants.INSTALL_DIR_NAME);
        String jarName = String.format(FullMigrationToolConstants.FULL_MIGRATION_JAR_NAME_MODEL,
                PortalConstants.PORTAL_VERSION);
        this.jarPath = String.format("%s/%s/%s", this.installDirPath,
                FullMigrationToolConstants.FULL_MIGRATION_JAR_HOME_NAME, jarName);
    }

    /**
     * Get instance of FullMigrationTool
     *
     * @return FullMigrationTool instance
     */
    public static FullMigrationTool getInstance() {
        if (instance == null) {
            synchronized (FullMigrationTool.class) {
                if (instance == null) {
                    instance = new FullMigrationTool();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Full-Migration tool is already installed");
            return;
        }

        LOGGER.info("Start to install Full-Migration tool");
        LOGGER.info("Create Full-Migration tool install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Copy Full-Migration tool jar to install directory");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check Full-Migration install files");
        checkKeyFileExists(jarPath);
        LOGGER.info("Install Full-Migration tool successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Full-Migration tool is not installed");
            return;
        }

        LOGGER.info("Start uninstall Full-Migration tool");
        deletePath(installDirPath);
        LOGGER.info("Uninstall Full-Migration tool successfully");
    }

    @Override
    public String getToolName() {
        return FullMigrationToolConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(jarPath);
        } catch (InstallException e) {
            return false;
        }
        return true;
    }
}
