/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.DebeziumConstants;
import org.opengauss.exceptions.InstallException;
import org.opengauss.config.ApplicationConfig;

/**
 * debezium
 *
 * @since 2025/2/17
 */
@Getter
public class Debezium extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(Debezium.class);

    private static volatile Debezium instance;

    private final String pkgDirPath;
    private final String connectMysqlPkgName;
    private final String connectOpenGaussPkgName;
    private final String connectPgsqlPkgName;
    private final String installDirPath;
    private final String connectMysqlJarPath;
    private final String connectOpenGaussJarPath;
    private final String connectPgsqlJarPath;

    private Debezium() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();
        String portalVersion = PortalConstants.PORTAL_VERSION;

        this.pkgDirPath = String.format("%s/%s", applicationConfig.getPortalPkgDirPath(),
                DebeziumConstants.INSTALL_PKG_DIR_NAME);
        this.connectMysqlPkgName = String.format(DebeziumConstants.CONNECT_MYSQL_INSTALL_PKG_NAME_MODEL, portalVersion);
        this.connectOpenGaussPkgName = String.format(DebeziumConstants.CONNECT_OPENGAUSS_INSTALL_PKG_NAME_MODEL,
                portalVersion);
        this.connectPgsqlPkgName = String.format(DebeziumConstants.CONNECT_PGSQL_INSTALL_PKG_NAME_MODEL,
                portalVersion);
        this.installDirPath = String.format("%s/%s", applicationConfig.getPortalToolsDirPath(),
                DebeziumConstants.INSTALL_DIR_NAME);
        this.connectMysqlJarPath = String.format("%s/%s", installDirPath,
                DebeziumConstants.CONNECT_MYSQL_JAR_RELATIVE_PATH);
        this.connectOpenGaussJarPath = String.format("%s/%s", installDirPath,
                DebeziumConstants.CONNECT_OPENGAUSS_JAR_RELATIVE_PATH);
        this.connectPgsqlJarPath = String.format("%s/%s", installDirPath,
                DebeziumConstants.CONNECT_PGSQL_JAR_RELATIVE_PATH);
    }

    /**
     * Get instance of Debezium
     *
     * @return instance of Debezium
     */
    public static Debezium getInstance() {
        if (instance == null) {
            synchronized (Debezium.class) {
                if (instance == null) {
                    instance = new Debezium();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Debezium is already installed");
            return;
        }

        LOGGER.info("Start to install Debezium");
        LOGGER.info("Create Debezium install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Unzip Debezium install package");
        unzipPackage(pkgDirPath, connectMysqlPkgName, installDirPath);
        unzipPackage(pkgDirPath, connectOpenGaussPkgName, installDirPath);
        unzipPackage(pkgDirPath, connectPgsqlPkgName, installDirPath);

        LOGGER.info("Check Debezium install files");
        checkKeyFileExists(connectMysqlJarPath);
        checkKeyFileExists(connectOpenGaussJarPath);
        checkKeyFileExists(connectPgsqlJarPath);
        LOGGER.info("Install Debezium successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Debezium is not installed");
            return;
        }

        LOGGER.info("Uninstall Debezium");
        deletePath(installDirPath);
        LOGGER.info("Uninstall Debezium successfully");
    }

    @Override
    public String getToolName() {
        return DebeziumConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(connectMysqlJarPath);
            checkKeyFileExists(connectOpenGaussJarPath);
            checkKeyFileExists(connectPgsqlJarPath);
        } catch (InstallException e) {
            return false;
        }
        return true;
    }
}
