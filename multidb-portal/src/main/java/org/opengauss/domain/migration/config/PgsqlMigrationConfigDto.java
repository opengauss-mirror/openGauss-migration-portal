/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.migration.config;

import lombok.Getter;
import org.opengauss.constants.ConfigValidationConstants;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.database.connect.info.DatabaseConnectInfo;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.ConsoleReader;
import org.opengauss.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * PostgreSQL migration configuration dto
 *
 * @since 2025/6/30
 */
@Getter
public class PgsqlMigrationConfigDto extends AbstractMigrationConfigDto {
    /**
     * PostgreSQL database configuration
     */
    private String pgsqlDatabaseIp;
    private String pgsqlDatabasePort;
    private String pgsqlDatabaseName;
    private String pgsqlDatabaseUsername;
    private String pgsqlDatabasePassword;
    private String pgsqlDatabaseSchemas;

    /**
     * openGauss database configuration
     */
    private String opengaussDatabaseIp;
    private String opengaussDatabasePort;
    private String opengaussDatabaseName;
    private String opengaussDatabaseUsername;
    private String opengaussDatabasePassword;

    /**
     * openGauss database standby nodes configuration
     */
    private String opengaussDatabaseStandbyHosts;
    private String opengaussDatabaseStandbyPorts;

    /**
     * schema mapping configuration
     */
    private String schemaMappings;

    /**
     * full migration process jvm configuration
     */
    private String fullProcessJvm;

    /**
     * incremental process jvm configuration
     */
    private String incrementalMigrationSourceProcessJvm;
    private String incrementalMigrationSinkProcessJvm;

    /**
     * reverse process jvm configuration
     */
    private String reverseMigrationSourceProcessJvm;
    private String reverseMigrationSinkProcessJvm;

    /**
     * Generate pgsql migration config dto
     *
     * @param configMap migration config map
     * @return PgsqlMigrationConfigDto
     */
    public static PgsqlMigrationConfigDto generatePgsqlMigrationConfigDto(Map<String, Object> configMap) {
        if (configMap == null) {
            throw new IllegalArgumentException(
                    "Config map that is used to generate PostgreSQL migration config dto cannot be null");
        }
        PgsqlMigrationConfigDto dto = new PgsqlMigrationConfigDto();
        dto.setMigrationMode(getConfigFromMap(MigrationConfig.MIGRATION_MODE, configMap));
        dto.setIsMigrationObject(getConfigFromMap(MigrationConfig.IS_MIGRATION_OBJECT, configMap, "true"));
        dto.setIsAdjustKernelParam(getConfigFromMap(MigrationConfig.IS_ADJUST_KERNEL_PARAM, configMap, "false"));

        readDatabasePassword(dto, configMap);
        dto.pgsqlDatabaseIp = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_IP, configMap);
        dto.pgsqlDatabasePort = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_PORT, configMap);
        dto.pgsqlDatabaseName = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_NAME, configMap);
        dto.pgsqlDatabaseUsername = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_USERNAME, configMap);
        dto.pgsqlDatabaseSchemas = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_SCHEMAS, configMap);

        dto.opengaussDatabaseIp = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_IP, configMap);
        dto.opengaussDatabasePort = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PORT, configMap);
        dto.opengaussDatabaseName = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_NAME, configMap);
        dto.opengaussDatabaseUsername = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_USERNAME, configMap);

        dto.opengaussDatabaseStandbyHosts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_HOSTS, configMap, "");
        dto.opengaussDatabaseStandbyPorts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_PORTS, configMap, "");

        dto.schemaMappings = getConfigFromMap(MigrationConfig.SCHEMA_MAPPINGS, configMap, "");

        dto.fullProcessJvm = getConfigFromMap(MigrationConfig.FULL_PROCESS_JVM, configMap);
        dto.incrementalMigrationSourceProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_MIGRATION_SOURCE_PROCESS_JVM, configMap);
        dto.incrementalMigrationSinkProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_MIGRATION_SINK_PROCESS_JVM, configMap);
        dto.reverseMigrationSourceProcessJvm =
                getConfigFromMap(MigrationConfig.REVERSE_MIGRATION_SOURCE_PROCESS_JVM, configMap);
        dto.reverseMigrationSinkProcessJvm =
                getConfigFromMap(MigrationConfig.REVERSE_MIGRATION_SINK_PROCESS_JVM, configMap);

        dto.validateConfig();
        return dto;
    }

    private static void readDatabasePassword(PgsqlMigrationConfigDto dto, Map<String, Object> configMap) {
        dto.setUseInteractivePassword(getConfigFromMap(MigrationConfig.USE_INTERACTIVE_PASSWORD, configMap, "false"));
        if (dto.isUseInteractivePassword()) {
            dto.pgsqlDatabasePassword = ConsoleReader.readPassword("Please input PostgreSQL database password: ");
            dto.opengaussDatabasePassword = ConsoleReader.readPassword("Please input openGauss database password: ");
        } else {
            dto.pgsqlDatabasePassword = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_PASSWORD, configMap);
            dto.opengaussDatabasePassword = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PASSWORD, configMap);
        }
    }

    @Override
    public boolean isOpenGaussClusterAvailable() {
        return isOpenGaussClusterAvailable(opengaussDatabaseStandbyHosts, opengaussDatabaseStandbyPorts);
    }

    /**
     * Get PostgreSQL database connect info
     *
     * @return DatabaseConnectInfo PostgreSQL database connect info
     */
    public DatabaseConnectInfo getPgsqlConnectInfo() {
        return new DatabaseConnectInfo(pgsqlDatabaseIp, pgsqlDatabasePort, pgsqlDatabaseName, pgsqlDatabaseUsername,
                pgsqlDatabasePassword);
    }

    /**
     * Get PostgreSQL connection
     *
     * @return Connection PostgreSQL connection
     * @throws SQLException if failed to get PostgreSQL connection
     * @throws ClassNotFoundException if failed to load PostgreSQL driver
     */
    public Connection getPgsqlConnection() throws SQLException, ClassNotFoundException {
        return JdbcUtils.getPgsqlConnection(pgsqlDatabaseIp, pgsqlDatabasePort, pgsqlDatabaseName,
                pgsqlDatabaseUsername, pgsqlDatabasePassword);
    }

    @Override
    public OpenGaussDatabaseConnectInfo getOpenGaussConnectInfo() {
        return new OpenGaussDatabaseConnectInfo(opengaussDatabaseIp, opengaussDatabasePort, opengaussDatabaseName,
                opengaussDatabaseUsername, opengaussDatabasePassword, isOpenGaussClusterAvailable(),
                opengaussDatabaseStandbyHosts, opengaussDatabaseStandbyPorts);
    }

    @Override
    public Connection getOpenGaussConnection() throws SQLException {
        return JdbcUtils.getOpengaussConnection(getOpenGaussConnectInfo());
    }

    @Override
    void validateConfig() {
        if (!ConfigValidationConstants.IP_REGEX.matcher(pgsqlDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(pgsqlDatabasePort).matches()) {
            throw new ConfigException("PostgreSQL database IP or Port is invalid");
        }

        if (!ConfigValidationConstants.IP_REGEX.matcher(opengaussDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussDatabasePort).matches()) {
            throw new ConfigException("openGauss database IP or port is invalid");
        }

        if (hasDuplicateNames(pgsqlDatabaseSchemas)) {
            throw new ConfigException("PostgreSQL database schemas contain duplicate names");
        }
    }
}
