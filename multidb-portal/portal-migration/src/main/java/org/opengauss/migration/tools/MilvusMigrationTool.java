/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.Portal;
import org.opengauss.constants.tool.MilvusMigrationToolConstants;
import org.opengauss.exceptions.InstallException;

/**
 * MilvusMigrationTool
 *
 * @since 2025/10/20
 */
@Getter
public class MilvusMigrationTool extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(MilvusMigrationTool.class);

    private static volatile MilvusMigrationTool instance;

    private final String pkgDirPath;
    private final String migrationPyName;
    private final String installDirPath;
    private final String migrationPyPath;

    private MilvusMigrationTool() {
        Portal portal = Portal.getInstance();

        this.pkgDirPath = String.format("%s/%s", portal.getPortalPkgDirPath(),
                MilvusMigrationToolConstants.INSTALL_PKG_DIR_NAME);
        this.migrationPyName = MilvusMigrationToolConstants.MIGRATION_PY_NAME;
        this.installDirPath = String.format("%s/%s", portal.getPortalToolsDirPath(),
                MilvusMigrationToolConstants.INSTALL_DIR_NAME);
        this.migrationPyPath = String.format("%s/%s", installDirPath, migrationPyName);
    }

    /**
     * Get instance of MilvusMigrationTool
     *
     * @return instance of MilvusMigrationTool
     */
    public static MilvusMigrationTool getInstance() {
        if (instance == null) {
            synchronized (MilvusMigrationTool.class) {
                if (instance == null) {
                    instance = new MilvusMigrationTool();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Milvus migration tool is already installed");
            return;
        }

        LOGGER.info("Start to install Milvus migration tool");
        LOGGER.info("Create Milvus migration tool install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Copy Milvus migration python script");
        copyFile(String.format("%s/%s", pkgDirPath, migrationPyName), migrationPyPath);

        LOGGER.info("Check Milvus migration tool key file exists");
        checkKeyFileExists(migrationPyPath);
        LOGGER.info("Install Milvus migration tool successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Milvus migration tool is not installed");
            return;
        }

        LOGGER.info("Start to uninstall Milvus migration tool");
        LOGGER.info("Delete Milvus migration tool install directory");
        deletePath(installDirPath);
        LOGGER.info("Uninstall Milvus migration tool successfully");
    }

    @Override
    public String getToolName() {
        return MilvusMigrationToolConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(migrationPyPath);
        } catch (InstallException e) {
            return false;
        }
        return true;
    }
}
