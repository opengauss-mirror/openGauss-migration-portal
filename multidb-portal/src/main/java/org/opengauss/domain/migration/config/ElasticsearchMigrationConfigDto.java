/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.migration.config;

import lombok.Getter;
import org.elasticsearch.client.RestClient;
import org.opengauss.constants.ConfigValidationConstants;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.database.connect.info.ElasticsearchConnectInfo;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.ConsoleReader;
import org.opengauss.utils.ElasticsearchUtils;
import org.opengauss.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Elasticsearch migration config dto
 *
 * @since 2025/10/20
 */
@Getter
public class ElasticsearchMigrationConfigDto extends AbstractMigrationConfigDto {
    /**
     * Migration control configuration
     */
    private String migrationConcurrentThreads;

    /**
     * Elasticsearch configuration
     */
    private String elasticsearchHost;
    private String elasticsearchIndexes;
    private String elasticsearchUsername;
    private String elasticsearchPassword;

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
     * Generate Elasticsearch migration config dto
     *
     * @param configMap migration config map
     * @return ElasticsearchMigrationConfigDto
     */
    public static ElasticsearchMigrationConfigDto generateElasticsearchMigrationConfigDto(
            Map<String, Object> configMap) {
        if (configMap == null) {
            throw new IllegalArgumentException(
                    "Config map that is used to generate Elasticsearch migration config dto cannot be null");
        }
        ElasticsearchMigrationConfigDto dto = new ElasticsearchMigrationConfigDto();
        dto.migrationConcurrentThreads = getConfigFromMap(MigrationConfig.MIGRATION_CONCURRENT_THREADS, configMap, "4");
        dto.setIsAdjustKernelParam(getConfigFromMap(MigrationConfig.IS_ADJUST_KERNEL_PARAM, configMap, "false"));

        readDatabasePassword(dto, configMap);
        dto.elasticsearchHost = getConfigFromMap(MigrationConfig.ELASTICSEARCH_HOST, configMap);
        dto.elasticsearchIndexes = getConfigFromMap(MigrationConfig.ELASTICSEARCH_INDEXES, configMap);
        dto.elasticsearchUsername = getConfigFromMap(MigrationConfig.ELASTICSEARCH_USERNAME, configMap, "");

        dto.opengaussDatabaseIp = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_IP, configMap);
        dto.opengaussDatabasePort = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PORT, configMap);
        dto.opengaussDatabaseName = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_NAME, configMap);
        dto.opengaussDatabaseUsername = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_USERNAME, configMap);

        dto.tableMappings = getConfigFromMap(MigrationConfig.TABLE_MAPPINGS, configMap, "");

        dto.validateConfig();
        return dto;
    }

    private static void readDatabasePassword(ElasticsearchMigrationConfigDto dto, Map<String, Object> configMap) {
        dto.setUseInteractivePassword(getConfigFromMap(MigrationConfig.USE_INTERACTIVE_PASSWORD, configMap, "false"));

        if (dto.isUseInteractivePassword()) {
            dto.opengaussDatabasePassword = ConsoleReader.readPassword("Please input openGauss database password: ");
        } else {
            dto.elasticsearchPassword = getConfigFromMap(MigrationConfig.ELASTICSEARCH_PASSWORD, configMap, "");
            dto.opengaussDatabasePassword = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PASSWORD, configMap);
        }
    }

    /**
     * Check if Elasticsearch anonymous access is enabled
     *
     * @return true if Elasticsearch anonymous access is enabled, false otherwise
     */
    public boolean isElasticsearchAnonymousAccess() {
        return elasticsearchUsername == null || elasticsearchUsername.isEmpty()
                || elasticsearchPassword == null || elasticsearchPassword.isEmpty();
    }

    /**
     * Get Elasticsearch connect info
     *
     * @return ElasticsearchConnectInfo
     */
    public ElasticsearchConnectInfo getElasticsearchConnectInfo() {
        return new ElasticsearchConnectInfo(elasticsearchHost, elasticsearchUsername, elasticsearchPassword);
    }

    /**
     * Get Elasticsearch rest client
     *
     * @return RestClient
     */
    public RestClient getElasticsearchRestClient() {
        ElasticsearchConnectInfo connectInfo = getElasticsearchConnectInfo();
        return ElasticsearchUtils.createRestClient(connectInfo.getIp(), Integer.parseInt(connectInfo.getPort()),
                connectInfo.getUsername(), connectInfo.getPassword());
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
        throw new UnsupportedOperationException("Elasticsearch migration mode is not supported");
    }

    @Override
    public String getIsMigrationObject() {
        throw new UnsupportedOperationException("Elasticsearch migration object is not supported");
    }

    @Override
    void validateConfig() {
        ElasticsearchConnectInfo connectInfo = getElasticsearchConnectInfo();
        String elasticsearchIp = connectInfo.getIp();
        String elasticsearchPort = connectInfo.getPort();

        if (!ConfigValidationConstants.IP_REGEX.matcher(elasticsearchIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(elasticsearchPort).matches()) {
            throw new ConfigException("Elasticsearch IP or Port is invalid");
        }

        if (!ConfigValidationConstants.IP_REGEX.matcher(opengaussDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussDatabasePort).matches()) {
            throw new ConfigException("openGauss database IP or port is invalid");
        }

        if (hasDuplicateNames(elasticsearchIndexes)) {
            throw new ConfigException("Elasticsearch indexes contain duplicate names");
        }
    }
}
