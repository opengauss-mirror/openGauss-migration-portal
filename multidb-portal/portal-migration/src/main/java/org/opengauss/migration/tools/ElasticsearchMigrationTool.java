/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.Portal;
import org.opengauss.constants.tool.ElasticsearchMigrationToolConstants;
import org.opengauss.exceptions.InstallException;

/**
 * ElasticsearchMigrationTool
 *
 * @since 2025/10/20
 */
@Getter
public class ElasticsearchMigrationTool extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchMigrationTool.class);

    private static volatile ElasticsearchMigrationTool instance;

    private final String pkgDirPath;
    private final String migrationPyName;
    private final String installDirPath;
    private final String migrationPyPath;

    private ElasticsearchMigrationTool() {
        Portal portal = Portal.getInstance();

        this.pkgDirPath = String.format("%s/%s", portal.getPortalPkgDirPath(),
                ElasticsearchMigrationToolConstants.INSTALL_PKG_DIR_NAME);
        this.migrationPyName = ElasticsearchMigrationToolConstants.MIGRATION_PY_NAME;
        this.installDirPath = String.format("%s/%s", portal.getPortalToolsDirPath(),
                ElasticsearchMigrationToolConstants.INSTALL_DIR_NAME);
        this.migrationPyPath = String.format("%s/%s", installDirPath, migrationPyName);
    }

    /**
     * Get instance of ElasticsearchMigrationTool
     *
     * @return instance of ElasticsearchMigrationTool
     */
    public static ElasticsearchMigrationTool getInstance() {
        if (instance == null) {
            synchronized (ElasticsearchMigrationTool.class) {
                if (instance == null) {
                    instance = new ElasticsearchMigrationTool();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Elasticsearch migration tool is already installed");
            return;
        }

        LOGGER.info("Start to install Elasticsearch migration tool");
        LOGGER.info("Create Elasticsearch migration tool install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Copy Elasticsearch migration python script");
        copyFile(String.format("%s/%s", pkgDirPath, migrationPyName), migrationPyPath);

        LOGGER.info("Check Elasticsearch migration tool key file exists");
        checkKeyFileExists(migrationPyPath);
        LOGGER.info("Install Elasticsearch migration tool successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Elasticsearch migration tool is not installed");
            return;
        }

        LOGGER.info("Start to uninstall Elasticsearch migration tool");
        LOGGER.info("Delete Elasticsearch migration tool install directory");
        deletePath(installDirPath);
        LOGGER.info("Uninstall Elasticsearch migration tool successfully");
    }

    @Override
    public String getToolName() {
        return ElasticsearchMigrationToolConstants.TOOL_NAME;
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
