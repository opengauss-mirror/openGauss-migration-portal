/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.dto;

import lombok.Getter;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.model.DatabaseConnectInfo;
import org.opengauss.domain.model.OpenGaussDatabaseConnectInfo;

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
     * @param migrationConfigMap migration config map
     * @return PgsqlMigrationConfigDto
     */
    public static PgsqlMigrationConfigDto generatePgsqlMigrationConfigDto(Map<String, Object> migrationConfigMap) {
        if (migrationConfigMap == null) {
            throw new IllegalArgumentException(
                    "Config map that is used to generate PostgreSQL migration config dto cannot be null");
        }
        PgsqlMigrationConfigDto dto = new PgsqlMigrationConfigDto();
        dto.migrationMode = getConfigFromMap(MigrationConfig.MIGRATION_MODE, migrationConfigMap);
        dto.isMigrationObject = getConfigFromMap(MigrationConfig.IS_MIGRATION_OBJECT, migrationConfigMap, "true");
        dto.isAdjustKernelParam = getConfigFromMap(MigrationConfig.IS_ADJUST_KERNEL_PARAM, migrationConfigMap, "false");

        dto.pgsqlDatabaseIp = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_IP, migrationConfigMap);
        dto.pgsqlDatabasePort = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_PORT, migrationConfigMap);
        dto.pgsqlDatabaseName = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_NAME, migrationConfigMap);
        dto.pgsqlDatabaseUsername = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_USERNAME, migrationConfigMap);
        dto.pgsqlDatabasePassword = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_PASSWORD, migrationConfigMap);
        dto.pgsqlDatabaseSchemas = getConfigFromMap(MigrationConfig.PGSQL_DATABASE_SCHEMAS, migrationConfigMap);

        dto.opengaussDatabaseIp = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_IP, migrationConfigMap);
        dto.opengaussDatabasePort = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PORT, migrationConfigMap);
        dto.opengaussDatabaseName = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_NAME, migrationConfigMap);
        dto.opengaussDatabaseUsername =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_USERNAME, migrationConfigMap);
        dto.opengaussDatabasePassword =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PASSWORD, migrationConfigMap);

        dto.opengaussDatabaseStandbyHosts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_HOSTS, migrationConfigMap, "");
        dto.opengaussDatabaseStandbyPorts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_PORTS, migrationConfigMap, "");

        dto.schemaMappings = getConfigFromMap(MigrationConfig.SCHEMA_MAPPINGS, migrationConfigMap, "");

        dto.fullProcessJvm = getConfigFromMap(MigrationConfig.FULL_PROCESS_JVM, migrationConfigMap);
        dto.incrementalMigrationSourceProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_MIGRATION_SOURCE_PROCESS_JVM, migrationConfigMap);
        dto.incrementalMigrationSinkProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_MIGRATION_SINK_PROCESS_JVM, migrationConfigMap);
        dto.reverseMigrationSourceProcessJvm =
                getConfigFromMap(MigrationConfig.REVERSE_MIGRATION_SOURCE_PROCESS_JVM, migrationConfigMap);
        dto.reverseMigrationSinkProcessJvm =
                getConfigFromMap(MigrationConfig.REVERSE_MIGRATION_SINK_PROCESS_JVM, migrationConfigMap);
        return dto;
    }

    /**
     * Check whether openGauss cluster is available
     *
     * @return true if openGauss cluster is available
     */
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

    @Override
    public OpenGaussDatabaseConnectInfo getOpenGaussConnectInfo() {
        return new OpenGaussDatabaseConnectInfo(opengaussDatabaseIp, opengaussDatabasePort, opengaussDatabaseName,
                opengaussDatabaseUsername, opengaussDatabasePassword, isOpenGaussClusterAvailable(),
                opengaussDatabaseStandbyHosts, opengaussDatabaseStandbyPorts);
    }
}
