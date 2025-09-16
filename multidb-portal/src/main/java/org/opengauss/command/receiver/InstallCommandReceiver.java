/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.PortalConstants;
import org.opengauss.exceptions.InstallException;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.migration.tools.DataChecker;
import org.opengauss.migration.tools.Debezium;
import org.opengauss.migration.tools.OgDatasync;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.ProcessUtils;

import java.io.IOException;

/**
 * install command receiver
 *
 * @since 2025/3/27
 */
public class InstallCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(InstallCommandReceiver.class);

    /**
     * install chameleon dependencies
     *
     * @param isForce force to install dependencies
     **/
    public void dependencies(boolean isForce) {
        LOGGER.info("Start to install dependencies");
        if (!isForce && !checkSystemAndArch()) {
            return;
        }
        LOGGER.info("Check user sudo permission");
        checkSudoPermission();
        installDependencies();
        LOGGER.info("Install dependencies finished");
    }

    /**
     * install all migration tools
     **/
    public void migrationTools() {
        checkLeastSpace();
        OgDatasync.getInstance().install();
        Chameleon.getInstance().install();
        DataChecker.getInstance().install();
        Debezium.getInstance().install();
        Kafka.getInstance().install();
        LOGGER.info("Install all migration tools successfully");
    }

    /**
     * install chameleon
     **/
    public void chameleon() {
        Chameleon.getInstance().install();
    }

    /**
     * install oG_datasync_full_migration
     **/
    public void ogDatasync() {
        OgDatasync.getInstance().install();
    }

    /**
     * install debezium
     **/
    public void debezium() {
        Debezium.getInstance().install();
    }

    /**
     * install data-checker
     **/
    public void dataChecker() {
        DataChecker.getInstance().install();
    }

    /**
     * install kafka
     **/
    public void kafka() {
        Kafka.getInstance().install();
    }

    /**
     * check all migration tools
     **/
    public void check() {
        boolean isAllInstalled = true;
        if (Chameleon.getInstance().checkInstall()) {
            LOGGER.info("Chameleon is already installed");
        } else {
            LOGGER.error("Chameleon is not installed");
            isAllInstalled = false;
        }

        if (OgDatasync.getInstance().checkInstall()) {
            LOGGER.info("oG_datasync_full_migration is already installed");
        } else {
            LOGGER.error("oG_datasync_full_migration is not installed");
            isAllInstalled = false;
        }

        if (DataChecker.getInstance().checkInstall()) {
            LOGGER.info("DataChecker is already installed");
        } else {
            LOGGER.error("DataChecker is not installed");
            isAllInstalled = false;
        }

        if (Debezium.getInstance().checkInstall()) {
            LOGGER.info("Debezium is already installed");
        } else {
            LOGGER.error("Debezium is not installed");
            isAllInstalled = false;
        }

        if (Kafka.getInstance().checkInstall()) {
            LOGGER.info("Kafka is already installed");
        } else {
            LOGGER.error("Kafka is not installed");
            isAllInstalled = false;
        }

        if (isAllInstalled) {
            LOGGER.info("All migration tools are already installed");
        } else {
            LOGGER.error("Some migration tools are not installed");
        }
    }

    private void checkLeastSpace() {
        LOGGER.info("Check space is sufficient");
        String portalHomeDir = ApplicationConfig.getInstance().getPortalHomeDirPath();
        try {
            if (!FileUtils.isSpaceSufficient(portalHomeDir, PortalConstants.LEAST_SPACE_MB)) {
                throw new InstallException("Not enough space in portal home directory to install migration tools, "
                        + "at least" + PortalConstants.LEAST_SPACE_MB + " MB is required");
            }
        } catch (IOException e) {
            throw new InstallException("Failed to check space is sufficient in portal home directory", e);
        }
    }

    private boolean checkSystemAndArch() {
        String osName = getSystemOs() + getSystemOsVersion();
        String osArch = getOsArch();

        String portalSystemName = ApplicationConfig.getInstance().getSystemName();
        String portalSystemArch = ApplicationConfig.getInstance().getSystemArch();

        if (!osName.equalsIgnoreCase(portalSystemName) || !osArch.equalsIgnoreCase(portalSystemArch)) {
            LOGGER.warn("System and architecture do not match, current portal install package supported "
                    + "system and architecture is {}_{}", portalSystemName, portalSystemArch);
            LOGGER.warn("Check current system and architecture is {}_{}", osName, osArch);
            LOGGER.warn("If you still want to install, you can add --force option to the end of the install command");
            return false;
        }
        LOGGER.debug("System and architecture match");
        return true;
    }

    private String getOsArch() {
        String arch = System.getProperty("os.arch").toLowerCase();
        if (arch.contains("aarch64")) {
            return "aarch64";
        } else if (arch.contains("x86_64") || arch.contains("amd64")) {
            return "x86_64";
        } else if (arch.contains("x86") || arch.contains("i386")) {
            return "x86";
        } else {
            return arch;
        }
    }

    private String getSystemOs() {
        try {
            return ProcessUtils.executeCommandWithResult(PortalConstants.COMMAND_OS).trim();
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to get system os", e);
        }
    }

    private String getSystemOsVersion() {
        try {
            return ProcessUtils.executeCommandWithResult(PortalConstants.COMMAND_OS_VERSION).trim();
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to get system os version", e);
        }
    }

    private void checkSudoPermission() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "/bin/bash", "-c", "sudo -n true &> /dev/null && echo 0 || echo 1"
            );
            String exitCode = ProcessUtils.executeCommandWithResult(processBuilder).trim();

            if (exitCode.equals("0")) {
                LOGGER.debug("The installation user has the sudo permission");
            } else {
                throw new InstallException("The installation user does not have the sudo permission, "
                        + "or a password is required.");
            }
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to check sudo permission", e);
        }
    }

    private void installDependencies() {
        LOGGER.info("Check dependencies install script");
        String installScriptName = PortalConstants.DEPENDENCIES_INSTALL_SCRIPT_NAME;
        String installScriptDirPath = String.format("%s/%s", ApplicationConfig.getInstance().getPortalPkgDirPath(),
                PortalConstants.DEPENDENCIES_INSTALL_SCRIPT_DIR_RELATIVE_PATH);
        String installScriptPath = String.format("%s/%s", installScriptDirPath, installScriptName);
        if (!FileUtils.checkFileExists(installScriptPath)) {
            throw new InstallException("Failed to install dependencies, required file not found - "
                    + installScriptPath);
        }

        try {
            LOGGER.info("Run dependencies install script");
            String installLogPath = String.format("%s/execute_%s.log", installScriptDirPath, installScriptName);
            String command = String.format("sh %s chameleon", installScriptName);
            ProcessUtils.executeCommand(command, installScriptDirPath, installLogPath, 60000L);
            String installLog = FileUtils.readFileContents(installLogPath);
            LOGGER.info("Install script logs: \n{}", installLog);
        } catch (IOException | InterruptedException e) {
            throw new InstallException("Failed to install dependencies", e);
        }
    }
}
