/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.OgDatasyncConstants;
import org.opengauss.exceptions.InstallException;

/**
 * OG_datasync_full_migration tool
 *
 * @since 2025/5/29
 */
@Getter
public class OgDatasync extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(OgDatasync.class);

    private static volatile OgDatasync instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;
    private final String jarPath;

    private OgDatasync() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();

        this.pkgDirPath = String.format("%s/%s", applicationConfig.getPortalPkgDirPath(),
                OgDatasyncConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = String.format(OgDatasyncConstants.INSTALL_PKG_NAME, PortalConstants.PORTAL_VERSION);
        this.installDirPath = String.format("%s/%s", applicationConfig.getPortalToolsDirPath(),
                OgDatasyncConstants.INSTALL_DIR_NAME);
        String jarName = String.format(OgDatasyncConstants.OG_DATASYNC_JAR_NAME_MODEL,
                PortalConstants.PORTAL_VERSION);
        this.jarPath = String.format("%s/%s/%s", this.installDirPath,
                OgDatasyncConstants.OG_DATASYNC_JAR_HOME_NAME, jarName);
    }

    /**
     * Get instance of FullMigrationTool
     *
     * @return FullMigrationTool instance
     */
    public static OgDatasync getInstance() {
        if (instance == null) {
            synchronized (OgDatasync.class) {
                if (instance == null) {
                    instance = new OgDatasync();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("OG_datasync_full_migration tool is already installed");
            return;
        }

        LOGGER.info("Start to install OG_datasync_full_migration tool");
        LOGGER.info("Create OG_datasync_full_migration tool install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Copy OG_datasync_full_migration tool jar to install directory");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check OG_datasync_full_migration install files");
        checkKeyFileExists(jarPath);
        LOGGER.info("Install OG_datasync_full_migration tool successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("OG_datasync_full_migration tool is not installed");
            return;
        }

        LOGGER.info("Start uninstall OG_datasync_full_migration tool");
        deletePath(installDirPath);
        LOGGER.info("Uninstall OG_datasync_full_migration tool successfully");
    }

    @Override
    public String getToolName() {
        return OgDatasyncConstants.TOOL_NAME;
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
