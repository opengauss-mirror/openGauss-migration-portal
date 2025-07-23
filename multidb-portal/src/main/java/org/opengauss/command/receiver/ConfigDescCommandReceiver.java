/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.exceptions.PortalException;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.utils.FileUtils;

import java.io.IOException;

/**
 * Config description command receiver
 *
 * @since 2025/6/24
 */
public class ConfigDescCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(ConfigDescCommandReceiver.class);

    /**
     * Export mysql config desc
     */
    public void mysqlConfigDesc() {
        exportDescFile(TemplateConfigType.MYSQL_MIGRATION_CONFIG);
    }

    /**
     * Export pgsql config desc
     */
    public void pgsqlConfigDesc() {
        exportDescFile(TemplateConfigType.PGSQL_MIGRATION_CONFIG);
    }

    private void exportDescFile(TemplateConfigType configType) {
        String configFilePath = configType.getFilePath();
        String configDescFilePath = configType.getConfigDescFilePath();
        String targetDirPath = ApplicationConfig.getInstance().getPortalTmpDirPath();
        String targetConfigFilePath = String.format("%s/%s", targetDirPath, configType.getName());
        String targetConfigDescFilePath = String.format("%s/%s", targetDirPath, configType.getConfigDescFileName());

        try {
            FileUtils.exportResource(configFilePath, targetConfigFilePath);
            FileUtils.exportResource(configDescFilePath, targetConfigDescFilePath);
        } catch (IOException e) {
            throw new PortalException("Failed to export config desc file", e);
        }
        LOGGER.info("Config description exported successfully");
        LOGGER.info("Config file path: {}", targetConfigFilePath);
        LOGGER.info("Config description file path: {}", targetConfigDescFilePath);
    }
}
