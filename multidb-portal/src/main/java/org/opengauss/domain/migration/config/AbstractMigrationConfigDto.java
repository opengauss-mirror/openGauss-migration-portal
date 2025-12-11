/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.migration.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.utils.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Abstract migration configuration dto
 *
 * @since 2025/6/30
 */
@Getter
@Setter
public abstract class AbstractMigrationConfigDto {
    private static final Logger LOGGER = LogManager.getLogger(AbstractMigrationConfigDto.class);

    /**
     * Migration mode
     */
    private String migrationMode;

    /**
     * Is migration object
     */
    private String isMigrationObject;

    /**
     * Is adjust kernel param
     */
    private String isAdjustKernelParam;

    /**
     * Whether to use interactive password input
     */
    private String useInteractivePassword;

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
     * Check for duplicate data in the names string
     *
     * @param namesStr names string, for example: name1,name2,name3,...
     * @return has duplicate name
     */
    protected static boolean hasDuplicateNames(String namesStr) {
        if (namesStr == null || namesStr.trim().isEmpty()) {
            return false;
        }

        String[] names = namesStr.split(",");
        Set<String> nameSet = new HashSet<>();

        for (String name : names) {
            String trimmedName = name.trim();
            if (!trimmedName.isEmpty()) {
                if (!nameSet.add(trimmedName)) {
                    return true;
                }
            }
        }
        return false;
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
     * Check whether openGauss cluster is available
     *
     * @return true if openGauss cluster is available
     */
    public abstract boolean isOpenGaussClusterAvailable();

    /**
     * Get openGauss database connect info
     *
     * @return OpenGaussDatabaseConnectInfo openGauss database connect info
     */
    public abstract OpenGaussDatabaseConnectInfo getOpenGaussConnectInfo();

    /**
     * Get openGauss connection
     *
     * @return Connection openGauss connection
     * @throws SQLException if failed to get openGauss connection
     */
    public abstract Connection getOpenGaussConnection() throws SQLException;

    /**
     * Check whether to use interactive password input
     *
     * @return true if to use interactive password input
     */
    public boolean isUseInteractivePassword() {
        return "true".equals(useInteractivePassword);
    }

    abstract void validateConfig();
}
