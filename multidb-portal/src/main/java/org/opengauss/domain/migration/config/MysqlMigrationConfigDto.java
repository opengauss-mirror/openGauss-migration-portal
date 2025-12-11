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
 * MySQL migration configuration dto
 *
 * @since 2025/6/30
 */
@Getter
public class MysqlMigrationConfigDto extends AbstractMigrationConfigDto {
    /**
     * MySQL database configuration
     */
    private String mysqlDatabaseIp;
    private String mysqlDatabasePort;
    private String mysqlDatabaseName;
    private String mysqlDatabaseUsername;
    private String mysqlDatabasePassword;
    private String mysqlDatabaseTables;

    /**
     * openGauss database configuration
     */
    private String opengaussDatabaseIp;
    private String opengaussDatabasePort;
    private String opengaussDatabaseName;
    private String opengaussDatabaseUsername;
    private String opengaussDatabasePassword;
    private String opengaussDatabaseSchema;

    /**
     * openGauss database standby nodes configuration
     */
    private String opengaussDatabaseStandbyHosts;
    private String opengaussDatabaseStandbyPorts;

    /**
     * data check process jvm configuration
     */
    private String fullCheckSourceProcessJvm;
    private String fullCheckSinkProcessJvm;
    private String fullCheckCheckProcessJvm;
    private String incrementalCheckSourceProcessJvm;
    private String incrementalCheckSinkProcessJvm;
    private String incrementalCheckCheckProcessJvm;

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

    private MysqlMigrationConfigDto() {
    }

    /**
     * Generate mysql migration config dto
     *
     * @param configMap migration config map
     * @return MysqlMigrationConfigDto
     */
    public static MysqlMigrationConfigDto generateMysqlMigrationConfigDto(Map<String, Object> configMap) {
        if (configMap == null) {
            throw new IllegalArgumentException(
                    "Config map that is used to generate MySQL migration config dto cannot be null");
        }
        MysqlMigrationConfigDto dto = new MysqlMigrationConfigDto();
        dto.setMigrationMode(getConfigFromMap(MigrationConfig.MIGRATION_MODE, configMap));
        dto.setIsMigrationObject(getConfigFromMap(MigrationConfig.IS_MIGRATION_OBJECT, configMap, "true"));
        dto.setIsAdjustKernelParam(getConfigFromMap(MigrationConfig.IS_ADJUST_KERNEL_PARAM, configMap, "false"));

        readDatabasePassword(dto, configMap);
        dto.mysqlDatabaseIp = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_IP, configMap);
        dto.mysqlDatabasePort = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_PORT, configMap);
        String mysqlDbName = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_NAME, configMap);
        dto.mysqlDatabaseName = mysqlDbName;
        dto.mysqlDatabaseUsername = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_USERNAME, configMap);
        dto.mysqlDatabaseTables = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_TABLES, configMap, "");

        dto.opengaussDatabaseIp = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_IP, configMap);
        dto.opengaussDatabasePort = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PORT, configMap);
        dto.opengaussDatabaseName = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_NAME, configMap);
        dto.opengaussDatabaseUsername = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_USERNAME, configMap);
        dto.opengaussDatabaseSchema =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_SCHEMA, configMap, mysqlDbName);
        dto.opengaussDatabaseStandbyHosts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_HOSTS, configMap, "");
        dto.opengaussDatabaseStandbyPorts =
                getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_STANDBY_PORTS, configMap, "");

        dto.fullCheckSourceProcessJvm = getConfigFromMap(MigrationConfig.FULL_CHECK_SOURCE_PROCESS_JVM, configMap);
        dto.fullCheckSinkProcessJvm = getConfigFromMap(MigrationConfig.FULL_CHECK_SINK_PROCESS_JVM, configMap);
        dto.fullCheckCheckProcessJvm = getConfigFromMap(MigrationConfig.FULL_CHECK_CHECK_PROCESS_JVM, configMap);
        dto.incrementalCheckSourceProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_CHECK_SOURCE_PROCESS_JVM, configMap);
        dto.incrementalCheckSinkProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_CHECK_SINK_PROCESS_JVM, configMap);
        dto.incrementalCheckCheckProcessJvm =
                getConfigFromMap(MigrationConfig.INCREMENTAL_CHECK_CHECK_PROCESS_JVM, configMap);
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

    private static void readDatabasePassword(MysqlMigrationConfigDto dto, Map<String, Object> configMap) {
        dto.setUseInteractivePassword(getConfigFromMap(MigrationConfig.USE_INTERACTIVE_PASSWORD, configMap, "false"));
        if (dto.isUseInteractivePassword()) {
            dto.mysqlDatabasePassword = ConsoleReader.readPassword("Please input MySQL database password: ");
            dto.opengaussDatabasePassword = ConsoleReader.readPassword("Please input openGauss database password: ");
        } else {
            dto.mysqlDatabasePassword = getConfigFromMap(MigrationConfig.MYSQL_DATABASE_PASSWORD, configMap);
            dto.opengaussDatabasePassword = getConfigFromMap(MigrationConfig.OPENGAUSS_DATABASE_PASSWORD, configMap);
        }
    }

    @Override
    public boolean isOpenGaussClusterAvailable() {
        return isOpenGaussClusterAvailable(opengaussDatabaseStandbyHosts, opengaussDatabaseStandbyPorts);
    }

    /**
     * Get mysql database connect info
     *
     * @return DatabaseConnectInfo mysql database connect info
     */
    public DatabaseConnectInfo getMysqlConnectInfo() {
        return new DatabaseConnectInfo(mysqlDatabaseIp, mysqlDatabasePort, mysqlDatabaseName,
                mysqlDatabaseUsername, mysqlDatabasePassword);
    }

    /**
     * Get mysql connection
     *
     * @return Connection mysql connection
     * @throws SQLException if failed to get mysql connection
     * @throws ClassNotFoundException if failed to load mysql driver
     */
    public Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
        return JdbcUtils.getMysqlConnection(mysqlDatabaseIp, mysqlDatabasePort, mysqlDatabaseName,
                mysqlDatabaseUsername, mysqlDatabasePassword);
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
        if (!ConfigValidationConstants.IP_REGEX.matcher(mysqlDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(mysqlDatabasePort).matches()) {
            throw new ConfigException("MySQL database IP or Port is invalid");
        }

        if (!ConfigValidationConstants.IP_REGEX.matcher(opengaussDatabaseIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussDatabasePort).matches()) {
            throw new ConfigException("openGauss database IP or port is invalid");
        }

        if (hasDuplicateNames(mysqlDatabaseTables)) {
            throw new ConfigException("MySQL database tables contain duplicate names");
        }
    }
}
