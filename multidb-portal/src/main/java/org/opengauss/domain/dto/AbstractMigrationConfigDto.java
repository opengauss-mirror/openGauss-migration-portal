/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.dto;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.model.OpenGaussDatabaseConnectInfo;
import org.opengauss.utils.StringUtils;

import java.util.Map;

/**
 * Abstract migration configuration dto
 *
 * @since 2025/6/30
 */
@Getter
public abstract class AbstractMigrationConfigDto {
    private static final Logger LOGGER = LogManager.getLogger(AbstractMigrationConfigDto.class);

    /**
     * Migration mode
     */
    protected String migrationMode;

    /**
     * Is migration object
     */
    protected String isMigrationObject;

    /**
     * Is adjust kernel param
     */
    protected String isAdjustKernelParam;

    /**
     * Whether to use interactive password input
     */
    protected String useInteractivePassword;

    /**
     * Get config from map
     *
     * @param key config key
     * @param configMap config map
     * @return config value
     */
    protected static String getConfigFromMap(String key, Map<String, Object> configMap) {
        Object value = configMap.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Migration config key '" + key + "' cannot be null");
        }
        return value.toString();
    }

    /**
     * Get config from map, if value is null, return default value
     *
     * @param key config key
     * @param configMap config map
     * @param defaultValue default value
     * @return config value
     */
    protected static String getConfigFromMap(String key, Map<String, Object> configMap, String defaultValue) {
        Object value = configMap.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value.toString().trim();
    }

    /**
     * Check whether the openGauss cluster is available
     *
     * @param hosts openGauss cluster hostnames
     * @param ports openGauss cluster ports
     * @return true if the openGauss cluster is available
     */
    protected boolean isOpenGaussClusterAvailable(String hosts, String ports) {
        if (StringUtils.isNullOrBlank(hosts) || StringUtils.isNullOrBlank(ports)) {
            return false;
        }

        if (hosts.split(",").length != ports.split(",").length) {
            LOGGER.warn("The number of hostname in {} does not match the number of port in {}",
                    MigrationConfig.OPENGAUSS_DATABASE_STANDBY_HOSTS, MigrationConfig.OPENGAUSS_DATABASE_STANDBY_PORTS);
            return false;
        }
        return true;
    }

    /**
     * Get openGauss database connect info
     *
     * @return OpenGaussDatabaseConnectInfo openGauss database connect info
     */
    public abstract OpenGaussDatabaseConnectInfo getOpenGaussConnectInfo();

    /**
     * Check whether to use interactive password input
     *
     * @return true if to use interactive password input
     */
    public boolean isUseInteractivePassword() {
        return "true".equals(useInteractivePassword);
    }
}
