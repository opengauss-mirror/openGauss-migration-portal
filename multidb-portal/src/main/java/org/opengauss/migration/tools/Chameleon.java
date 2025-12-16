/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.PortalConstants;
import org.opengauss.constants.tool.ChameleonConstants;
import org.opengauss.exceptions.InstallException;
import org.opengauss.config.Portal;
import org.opengauss.utils.ProcessUtils;

import java.io.IOException;

/**
 * chameleon
 *
 * @since 2025/2/17
 */
@Getter
public class Chameleon extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(Chameleon.class);

    private static volatile Chameleon instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;
    private final String chameleonHomeDirPath;
    private final String pgChameleonDirPath;
    private final String chameleonPath;
    private final String chameleonVersion;

    private Chameleon() {
        Portal portal = Portal.getInstance();
        String portalVersion = PortalConstants.PORTAL_VERSION;

        this.pkgDirPath = String.format("%s/%s", portal.getPortalPkgDirPath(),
                ChameleonConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = String.format(ChameleonConstants.INSTALL_PKG_NAME_MODEL, portalVersion,
                portal.getSystemArch());
        this.installDirPath = String.format("%s/%s", portal.getPortalToolsDirPath(),
                ChameleonConstants.INSTALL_DIR_NAME);

        String chameleonDirName = String.format(ChameleonConstants.CHAMELEON_DIR_HOME_NAME_MODEL, portalVersion);
        this.chameleonHomeDirPath = String.format("%s/%s", installDirPath, chameleonDirName);
        this.pgChameleonDirPath = ChameleonConstants.PG_CHAMELEON_DIR_PATH.replace("~",
                System.getProperty("user.home"));
        this.chameleonPath = String.format("%s/%s", chameleonHomeDirPath,
                ChameleonConstants.CHAMELEON_FILE_RELATIVE_PATH);
        this.chameleonVersion = portalVersion;
    }

    /**
     * Get instance of Chameleon
     *
     * @return instance of Chameleon
     */
    public static Chameleon getInstance() {
        if (instance == null) {
            synchronized (Chameleon.class) {
                if (instance == null) {
                    instance = new Chameleon();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Chameleon is already installed");
            return;
        }

        LOGGER.info("Start to install Chameleon");
        LOGGER.info("Create Chameleon install directory");
        createInstallDirPath(installDirPath);
        createInstallDirPath(pgChameleonDirPath);

        LOGGER.info("Unzip Chameleon install package");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check Chameleon install script");
        String installScriptName = "install.sh";
        String installScriptPath = String.format("%s/%s", chameleonHomeDirPath, installScriptName);

        LOGGER.info("Run Chameleon install script");
        checkKeyFileExists(installScriptPath);
        runInstallScript(installScriptName, installScriptPath);

        LOGGER.info("Check Chameleon install files");
        checkKeyFileExists(chameleonPath);

        LOGGER.info("Check Chameleon version");
        checkChameleonVersion();
        LOGGER.info("Install Chameleon successfully");
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Chameleon is not installed");
            return;
        }

        LOGGER.info("Uninstall Chameleon");

        String clearEnvScriptName = "clear_env_var.sh";
        String clearCommand = "sh " + clearEnvScriptName;
        try {
            ProcessUtils.executeCommand(clearCommand, chameleonHomeDirPath);
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Clear Chameleon environment variable in .bashrc failed, you can manually clear it");
        }

        deletePath(installDirPath);
        deletePath(pgChameleonDirPath);
        LOGGER.info("Uninstall Chameleon successfully");
    }

    @Override
    public String getToolName() {
        return ChameleonConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(chameleonPath);
            checkChameleonVersion();
        } catch (InstallException e) {
            return false;
        }
        return true;
    }

    private void runInstallScript(String installScriptName, String installScriptPath) {
        try {
            String logPath = String.format("%s/execute_%s.log", chameleonHomeDirPath, installScriptName);
            ProcessUtils.executeShellScript(installScriptName, chameleonHomeDirPath, logPath, 300000);
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to run Chameleon install script: " + installScriptPath, e);
        }
    }

    private void checkChameleonVersion() {
        try {
            String[] checkVersionCommand = {chameleonPath, "--version"};
            String checkResult = ProcessUtils.executeCommandWithResult(checkVersionCommand, chameleonHomeDirPath);
            String checkString = "chameleon " + chameleonVersion;
            if (!checkResult.contains(checkString)) {
                throw new InstallException("Failed to check Chameleon version, check result: " + checkResult);
            }
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to check Chameleon version", e);
        }
    }
}
