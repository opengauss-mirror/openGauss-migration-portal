/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.migration.config;

import io.milvus.v2.client.MilvusClientV2;
import lombok.Getter;
import org.opengauss.constants.ConfigValidationConstants;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.database.connect.info.DatabaseConnectInfo;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.ConsoleReader;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.MilvusUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Milvus migration config dto
 *
 * @since 2025/10/20
 */
@Getter
public class MilvusMigrationConfigDto extends AbstractMigrationConfigDto {
    /**
     * Migration control configuration
     */
    private String migrationConcurrentThreads;

    /**
     * Milvus configuration
     */
    private String milvusIp;
    private String milvusPort;
    private String milvusDatabase;
    private String milvusCollections;
    private String milvusUsername;
    private String milvusPassword;

    /**
     * openGauss database configuration
     */
    private String opengaussDatabaseIp;
    private String opengaussDatabasePort;
    private String opengaussDatabaseName;
    private String opengaussDatabaseUsername;
    private String opengaussDatabasePassword;

    /**
     * table mapping configuration
     */
    private String tableMappings;

    /**
     * Generate Milvus migration config dto
     *
     * @param configMap migration config map
     * @return MilvusMigrationConfigDto
     */
    public static MilvusMigrationConfigDto generateMilvusMigrationConfigDto(Map<String, Object> configMap) {
        if (configMap == null) {
            throw new IllegalArgumentException(
                    "Config map that is used to generate Milvus migration config dto cannot be null");
        }
        MilvusMigrationConfigDto dto = new MilvusMigrationConfigDto();
        dto.migrationConcurrentThreads = getConfigFromMap(MigrationConfig.MIGRATION_CONCURRENT_THREADS, configMap, "4");
        dto.setIsAdjustKernelParam(getConfigFromMap(MigrationConfig.IS_ADJUST_KERNEL_PARAM, configMap, "false"));

        readDatabasePassword(dto, configMap);
        dto.milvusIp = getConfigFromMap(MigrationConfig.MILVUS_IP, configMap);
        dto.milvusPort = getConfigFromMap(MigrationConfig.MILVUS_PORT, configMap);
        dto.milvusDatabase = getConfigFromMap(MigrationConfig.MILVUS_DATABASE, configMap, "default");
        dto.milvusCollections = getConfigFromMap(MigrationConfig.MILVUS_COLLECTIONS, configMap);
        dto.milvusUsername = getConfigFromMap(MigrationConfig.MILVUS_USERNAME, configMap, "");

        dto.opengaussDatabaseIp = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_IP, configMap);
        dto.opengaussDatabasePort = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PORT, configMap);
        dto.opengaussDatabaseName = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_NAME, configMap);
        dto.opengaussDatabaseUsername = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_USERNAME, configMap);

        dto.tableMappings = getConfigFromMap(MigrationConfig.TABLE_MAPPINGS, configMap, "");

        dto.validateConfig();
        return dto;
    }

    private static void readDatabasePassword(MilvusMigrationConfigDto dto, Map<String, Object> configMap) {
        dto.setUseInteractivePassword(getConfigFromMap(MigrationConfig.USE_INTERACTIVE_PASSWORD, configMap, "false"));

        if (dto.isUseInteractivePassword()) {
            dto.opengaussDatabasePassword = ConsoleReader.readPassword("Please input openGauss database password: ");
        } else {
            dto.milvusPassword = getConfigFromMap(MigrationConfig.MILVUS_PASSWORD, configMap, "");
            dto.opengaussDatabasePassword = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PASSWORD, configMap);
        }
    }

    /**
     * Check if Milvus anonymous access is enabled
     *
     * @return true if Milvus anonymous access is enabled, false otherwise
     */
    public boolean isMilvusAnonymousAccess() {
        return milvusUsername == null || milvusUsername.isEmpty() || milvusPassword == null || milvusPassword.isEmpty();
    }

    /**
     * Get Milvus connect info
     *
     * @return DatabaseConnectInfo Milvus connect info
     */
    public DatabaseConnectInfo getMilvusConnectInfo() {
        return new DatabaseConnectInfo(milvusIp, milvusPort, milvusDatabase, milvusUsername, milvusPassword);
    }

    /**
     * Get Milvus client V2
     *
     * @return MilvusClientV2 Milvus client V2
     */
    public MilvusClientV2 getMilvusClientV2() {
        return MilvusUtils.createMilvusClientV2(
                milvusIp, Integer.parseInt(milvusPort), milvusDatabase, milvusUsername, milvusPassword
        );
    }

    @Override
    public boolean isOpenGaussClusterAvailable() {
        return false;
    }

    @Override
    public OpenGaussDatabaseConnectInfo getOpenGaussConnectInfo() {
        return new OpenGaussDatabaseConnectInfo(opengaussDatabaseIp, opengaussDatabasePort, opengaussDatabaseName,
                opengaussDatabaseUsername, opengaussDatabasePassword, false, "", "");
    }

    @Override
    public Connection getOpenGaussConnection() throws SQLException {
        return JdbcUtils.getOpengaussConnection(opengaussDatabaseIp, opengaussDatabasePort, opengaussDatabaseName,
                opengaussDatabaseUsername, opengaussDatabasePassword);
    }

    @Override
    public String getMigrationMode() {
        throw new UnsupportedOperationException("Milvus migration mode is not supported");
    }

    @Override
    public String getIsMigrationObject() {
        throw new UnsupportedOperationException("Milvus migration object is not supported");
    }

    @Override
    void validateConfig() {
        if (!ConfigValidationConstants.IP_REGEX.matcher(milvusIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(milvusPort).matches()) {
            throw new ConfigException("Milvus IP or Port is invalid");
        }

        if (!ConfigValidationConstants.IP_REGEX.matcher(opengaussDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussDatabasePort).matches()) {
            throw new ConfigException("openGauss database IP or port is invalid");
        }

        if (hasDuplicateNames(milvusCollections)) {
            throw new ConfigException("Milvus collections contain duplicate names");
        }
    }
}
