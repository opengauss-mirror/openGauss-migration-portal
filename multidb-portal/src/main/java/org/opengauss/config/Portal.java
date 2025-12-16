/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.config;

import lombok.Getter;
import org.opengauss.constants.PortalConstants;
import org.opengauss.exceptions.PortalException;
import org.opengauss.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Portal
 *
 * @since 2025/3/21
 */
@Getter
public class Portal {
    private static volatile Portal instance;

    private String portalHomeDirPath;
    private String systemName;
    private String systemArch;

    private Portal() {}

    /**
     * Get instance of Portal
     *
     * @return instance of Portal
     */
    public static Portal getInstance() {
        if (instance == null) {
            synchronized (Portal.class) {
                if (instance == null) {
                    instance = new Portal();
                    instance.loadConfig();
                    instance.initPortalDir();
                }
            }
        }

        return instance;
    }

    /**
     * Get portal bin dir path
     *
     * @return String portal bin dir path
     */
    public String getPortalBinDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.BIN_DIR_NANE);
    }

    /**
     * Get portal config dir path
     *
     * @return String portal config dir path
     */
    public String getPortalConfigDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.CONFIG_DIR_NANE);
    }

    /**
     * Get portal data dir path
     *
     * @return String portal data dir path
     */
    public String getPortalDataDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.DATA_DIR_NANE);
    }

    /**
     * Get portal logs dir path
     *
     * @return String portal logs dir path
     */
    public String getPortalLogsDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.LOGS_DIR_NANE);
    }

    /**
     * Get portal pkg dir path
     *
     * @return String portal pkg dir path
     */
    public String getPortalPkgDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.PKG_DIR_NANE);
    }

    /**
     * Get portal template dir path
     *
     * @return String portal template dir path
     */
    public String getPortalTemplateDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.TEMPLATE_DIR_NANE);
    }

    /**
     * Get portal tmp dir path
     *
     * @return String portal tmp dir path
     */
    public String getPortalTmpDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.TMP_DIR_NANE);
    }

    /**
     * Get portal tools dir path
     *
     * @return String portal tools dir path
     */
    public String getPortalToolsDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.TOOLS_DIR_NANE);
    }

    /**
     * Get portal workspace dir path
     *
     * @return String portal workspace dir path
     */
    public String getPortalWorkspaceDirPath() {
        return String.format("%s/%s", portalHomeDirPath, PortalConstants.WORKSPACE_DIR_NANE);
    }

    private void loadConfig() {
        instance.portalHomeDirPath = loadPortalHomeDir();

        String configPath = instance.portalHomeDirPath + "/config/application.properties";
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
        } catch (IOException e) {
            throw new PortalException("Load portal application config failed, file path: " + configPath, e);
        }

        instance.systemName = properties.getProperty("system.name");
        instance.systemArch = properties.getProperty("system.arch");
    }

    private void initPortalDir() {
        String[] dirs = {
                getPortalBinDirPath(),
                getPortalConfigDirPath(),
                getPortalDataDirPath(),
                getPortalLogsDirPath(),
                getPortalPkgDirPath(),
                getPortalTemplateDirPath(),
                getPortalTmpDirPath(),
                getPortalToolsDirPath(),
                getPortalWorkspaceDirPath()
        };

        try {
            FileUtils.createDirectories(dirs);
        } catch (IOException e) {
            throw new PortalException("Create portal directories failed", e);
        }
    }

    private static String loadPortalHomeDir() {
        String classPath = Portal.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        return new File(classPath).getParent();
    }
}
