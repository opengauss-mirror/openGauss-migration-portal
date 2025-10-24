/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.FullReplicateConstants;
import org.opengauss.exceptions.InstallException;

/**
 * OG_datasync_full_migration tool
 *
 * @since 2025/5/29
 */
@Getter
public class FullReplicateTool extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(FullReplicateTool.class);

    private static volatile FullReplicateTool instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;
    private final String jarPath;

    private FullReplicateTool() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();

        this.pkgDirPath = String.format("%s/%s", applicationConfig.getPortalPkgDirPath(),
                FullReplicateConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = String.format(FullReplicateConstants.INSTALL_PKG_NAME, PortalConstants.PORTAL_VERSION);
        this.installDirPath = String.format("%s/%s", applicationConfig.getPortalToolsDirPath(),
                FullReplicateConstants.INSTALL_DIR_NAME);
        String jarName = String.format(FullReplicateConstants.FULL_REPLICATE_JAR_NAME_MODEL,
                PortalConstants.PORTAL_VERSION);
        this.jarPath = String.format("%s/%s/%s", this.installDirPath,
                FullReplicateConstants.FULL_REPLICATE_JAR_HOME_NAME, jarName);
    }

    /**
     * Get instance of FullMigrationTool
     *
     * @return FullMigrationTool instance
     */
    public static FullReplicateTool getInstance() {
        if (instance == null) {
            synchronized (FullReplicateTool.class) {
                if (instance == null) {
                    instance = new FullReplicateTool();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("openGauss-FullReplicate tool is already installed");
            return;
        }

        LOGGER.info("Start to install openGauss-FullReplicate tool");
        LOGGER.info("Create openGauss-FullReplicate tool install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Copy openGauss-FullReplicate tool jar to install directory");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check openGauss-FullReplicate install files");
        checkKeyFileExists(jarPath);
        LOGGER.info("Install openGauss-FullReplicate tool successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("openGauss-FullReplicate tool is not installed");
            return;
        }

        LOGGER.info("Start uninstall openGauss-FullReplicate tool");
        deletePath(installDirPath);
        LOGGER.info("Uninstall openGauss-FullReplicate tool successfully");
    }

    @Override
    public String getToolName() {
        return FullReplicateConstants.TOOL_NAME;
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
