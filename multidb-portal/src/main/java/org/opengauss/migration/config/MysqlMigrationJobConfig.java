/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.config;

import lombok.Getter;
import org.opengauss.constants.ConfigValidationConstants;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.model.ChameleonConfigBundle;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.DataCheckerConfigBundle;
import org.opengauss.domain.model.DebeziumConfigBundle;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DebeziumProcessType;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.migration.helper.config.ChameleonMysqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.DataCheckerMysqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.DebeziumMysqlMigrationConfigHelper;
import org.opengauss.migration.helper.tool.ChameleonHelper;
import org.opengauss.migration.helper.tool.DataCheckerHelper;

import java.util.Map;

/**
 * MySQL Migration Job Config
 *
 * @since 2025/7/2
 */
@Getter
public class MysqlMigrationJobConfig extends AbstractMigrationJobConfig {
    private final ChameleonConfigBundle fullConfigBundle;
    private final DataCheckerConfigBundle fullDataCheckConfigBundle;
    private final DataCheckerConfigBundle incrementalDataCheckConfigBundle;
    private final DebeziumConfigBundle incrementalConfigBundle;
    private final DebeziumConfigBundle reverseConfigBundle;

    private volatile MysqlMigrationConfigDto migrationConfigDto;

    public MysqlMigrationJobConfig(TaskWorkspace taskWorkspace) {
        super(taskWorkspace, new ConfigFile("migration.properties", taskWorkspace.getConfigDirPath(),
                taskWorkspace, TemplateConfigType.MYSQL_MIGRATION_CONFIG));

        this.fullConfigBundle = getFullConfigBundle(taskWorkspace);
        this.fullDataCheckConfigBundle = getFullDataCheckConfigBundle(taskWorkspace);
        this.incrementalConfigBundle = getIncrementalConfigBundle(taskWorkspace);
        this.incrementalDataCheckConfigBundle = getIncrementalDataCheckConfigBundle(taskWorkspace);
        this.reverseConfigBundle = getReverseConfigBundle(taskWorkspace);
    }

    /**
     * Get migration config dto
     *
     * @return mysql migration config dto
     */
    public MysqlMigrationConfigDto getMigrationConfigDto() {
        if (migrationConfigDto == null) {
            throw new IllegalStateException("MySQL migration config is not loaded");
        }
        return migrationConfigDto;
    }

    @Override
    public void loadConfig() {
        migrationConfigFile.loadConfigMap();
        migrationConfigDto = MysqlMigrationConfigDto.generateMysqlMigrationConfigDto(
                migrationConfigFile.getConfigMap());

        if (hasFullMigration()) {
            fullConfigBundle.loadConfigMap();
        }
        if (hasFullDataCheck()) {
            fullDataCheckConfigBundle.loadConfigMap();
        }

        if (hasIncrementalMigration()) {
            incrementalConfigBundle.loadConfigMap();
            if (hasIncrementalDataCheck()) {
                incrementalDataCheckConfigBundle.loadConfigMap();
            }
        }

        if (hasReverseMigration()) {
            reverseConfigBundle.loadConfigMap();
        }
    }

    @Override
    public void validateConfig() {
        Map<String, Object> migrationConfig = migrationConfigFile.getConfigMap();
        String mysqlIp = migrationConfig.get(MigrationConfig.MYSQL_DATABASE_IP).toString();
        String mysqlPort = migrationConfig.get(MigrationConfig.MYSQL_DATABASE_PORT).toString();
        String opengaussIp = migrationConfig.get(MigrationConfig.OPENGAUSS_DATABASE_IP).toString();
        String opengaussPort = migrationConfig.get(MigrationConfig.OPENGAUSS_DATABASE_PORT).toString();

        if (!ConfigValidationConstants.IP_REGEX.matcher(mysqlIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(mysqlPort).matches()
                || !ConfigValidationConstants.IP_REGEX.matcher(opengaussIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussPort).matches()) {
            throw new ConfigException("IP or Port is invalid");
        }
    }

    @Override
    public void changeToolsConfig() {
        if (hasFullMigration()) {
            changeFullConfig();
        }
        if (hasFullDataCheck()) {
            changeFullDataCheckConfig();
        }

        if (hasIncrementalMigration()) {
            changeIncrementalConfig();
            if (hasIncrementalDataCheck()) {
                changeIncrementalDataCheckConfig();
            }
        }

        if (hasReverseMigration()) {
            changeReverseConfig();
        }
    }

    @Override
    public void saveChangeConfig() {
        if (hasFullMigration()) {
            fullConfigBundle.saveConfigMap();
        }
        if (hasFullDataCheck()) {
            fullDataCheckConfigBundle.saveConfigMap();
        }

        if (hasIncrementalMigration()) {
            incrementalConfigBundle.saveConfigMap();
            if (hasIncrementalDataCheck()) {
                incrementalDataCheckConfigBundle.saveConfigMap();
            }
        }

        if (hasReverseMigration()) {
            reverseConfigBundle.saveConfigMap();
        }
    }

    @Override
    public void generateToolsConfigFiles() {
        migrationConfigFile.generateFile();
        fullConfigBundle.generateFile();
        fullDataCheckConfigBundle.generateFile();
        incrementalConfigBundle.generateFile();
        incrementalDataCheckConfigBundle.generateFile();
        reverseConfigBundle.generateFile();
    }

    private void changeFullConfig() {
        fullConfigBundle.getConfigFile().getConfigMap().putAll(
                ChameleonMysqlMigrationConfigHelper.mysqlFullMigrationConfig(migrationConfigDto, taskWorkspace));
    }

    private void changeFullDataCheckConfig() {
        String logConfigPath = fullDataCheckConfigBundle.getLog4j2ConfigFile().getFilePath();
        Map<String, Object> checkParams = DataCheckerMysqlMigrationConfigHelper.mysqlFullDataCheckCheckConfig(
                taskWorkspace, logConfigPath);
        Map<String, Object> sinkParams = DataCheckerMysqlMigrationConfigHelper.mysqlFullDataCheckSinkConfig(
                migrationConfigDto, logConfigPath);
        Map<String, Object> sourceParams = DataCheckerMysqlMigrationConfigHelper.mysqlFullDataCheckSourceConfig(
                migrationConfigDto, logConfigPath);
        Map<String, Object> log4j2Config = DataCheckerHelper.getFullCheckLog4j2Config(
                taskWorkspace);

        fullDataCheckConfigBundle.getCheckConfigFile().getConfigMap().putAll(checkParams);
        fullDataCheckConfigBundle.getSinkConfigFile().getConfigMap().putAll(sinkParams);
        fullDataCheckConfigBundle.getSourceConfigFile().getConfigMap().putAll(sourceParams);
        fullDataCheckConfigBundle.getLog4j2ConfigFile().getConfigMap().putAll(log4j2Config);
    }

    private void changeIncrementalConfig() {
        Map<String, Object> connectSourceParams = DebeziumMysqlMigrationConfigHelper.incrementalSourceConfig(
                migrationConfigDto, taskWorkspace);
        Map<String, Object> connectSinkParams = DebeziumMysqlMigrationConfigHelper.incrementalSinkConfig(
                migrationConfigDto, taskWorkspace);
        Map<String, Object> workerSourceParams = DebeziumMysqlMigrationConfigHelper.incrementalWorkerSourceConfig(
                taskWorkspace);
        Map<String, Object> workerSinkParams = DebeziumMysqlMigrationConfigHelper.incrementalWorkerSinkConfig(
                taskWorkspace);
        Map<String, Object> log4jSourceParams = DebeziumMysqlMigrationConfigHelper.incrementalLog4jConfig(
                taskWorkspace, DebeziumProcessType.SOURCE);
        Map<String, Object> log4jSinkParams = DebeziumMysqlMigrationConfigHelper.incrementalLog4jConfig(
                taskWorkspace, DebeziumProcessType.SINK);

        incrementalConfigBundle.getConnectSourceConfigFile().getConfigMap().putAll(connectSourceParams);
        incrementalConfigBundle.getConnectSinkConfigFile().getConfigMap().putAll(connectSinkParams);
        incrementalConfigBundle.getWorkerSourceConfigFile().getConfigMap().putAll(workerSourceParams);
        incrementalConfigBundle.getWorkerSinkConfigFile().getConfigMap().putAll(workerSinkParams);
        incrementalConfigBundle.getLog4jSourceConfigFile().getConfigMap().putAll(log4jSourceParams);
        incrementalConfigBundle.getLog4jSinkConfigFile().getConfigMap().putAll(log4jSinkParams);
    }

    private void changeIncrementalDataCheckConfig() {
        String logConfigPath = incrementalDataCheckConfigBundle.getLog4j2ConfigFile().getFilePath();
        String incrementalKafkaTopic = DebeziumMysqlMigrationConfigHelper.generateIncrementalKafkaTopic(taskWorkspace);

        Map<String, Object> checkParams = DataCheckerMysqlMigrationConfigHelper.mysqlIncrementalDataCheckCheckConfig(
                taskWorkspace, logConfigPath);
        incrementalDataCheckConfigBundle.getCheckConfigFile().getConfigMap().putAll(checkParams);
        Map<String, Object> sinkParams = DataCheckerMysqlMigrationConfigHelper.mysqlIncrementalDataCheckSinkConfig(
                migrationConfigDto, logConfigPath, incrementalKafkaTopic);
        incrementalDataCheckConfigBundle.getSinkConfigFile().getConfigMap().putAll(sinkParams);
        Map<String, Object> sourceParams = DataCheckerMysqlMigrationConfigHelper.mysqlIncrementalDataCheckSourceConfig(
                migrationConfigDto, logConfigPath, incrementalKafkaTopic);
        incrementalDataCheckConfigBundle.getSourceConfigFile().getConfigMap().putAll(sourceParams);

        Map<String, Object> log4j2Config = DataCheckerHelper.getIncrementalCheckLog4j2Config(
                taskWorkspace);
        incrementalDataCheckConfigBundle.getLog4j2ConfigFile().getConfigMap().putAll(log4j2Config);
    }

    private void changeReverseConfig() {
        Map<String, Object> connectSourceParams = DebeziumMysqlMigrationConfigHelper.reverseSourceConfig(
                migrationConfigDto, taskWorkspace);
        Map<String, Object> connectSinkParams = DebeziumMysqlMigrationConfigHelper.reverseSinkConfig(
                migrationConfigDto, taskWorkspace);
        reverseConfigBundle.getConnectSourceConfigFile().getConfigMap().putAll(connectSourceParams);
        reverseConfigBundle.getConnectSinkConfigFile().getConfigMap().putAll(connectSinkParams);

        Map<String, Object> workerSourceParams = DebeziumMysqlMigrationConfigHelper.reverseWorkerSourceConfig(
                taskWorkspace);
        Map<String, Object> workerSinkParams = DebeziumMysqlMigrationConfigHelper.reverseWorkerSinkConfig(
                taskWorkspace);
        reverseConfigBundle.getWorkerSourceConfigFile().getConfigMap().putAll(workerSourceParams);
        reverseConfigBundle.getWorkerSinkConfigFile().getConfigMap().putAll(workerSinkParams);

        Map<String, Object> log4jSourceParams = DebeziumMysqlMigrationConfigHelper.reverseLog4jConfig(taskWorkspace,
                DebeziumProcessType.SOURCE);
        Map<String, Object> log4jSinkParams = DebeziumMysqlMigrationConfigHelper.reverseLog4jConfig(taskWorkspace,
                DebeziumProcessType.SINK);
        reverseConfigBundle.getLog4jSourceConfigFile().getConfigMap().putAll(log4jSourceParams);
        reverseConfigBundle.getLog4jSinkConfigFile().getConfigMap().putAll(log4jSinkParams);
    }

    private ChameleonConfigBundle getFullConfigBundle(TaskWorkspace taskWorkspace) {
        ChameleonConfigBundle result = new ChameleonConfigBundle();
        String fullConfigName = ChameleonHelper.generateFullMigrationConfigFileName(taskWorkspace);
        result.setConfigFile(new ConfigFile(fullConfigName, taskWorkspace.getConfigFullDirPath(), taskWorkspace,
                TemplateConfigType.CHAMELEON_CONFIG));
        return result;
    }

    private DataCheckerConfigBundle getFullDataCheckConfigBundle(TaskWorkspace taskWorkspace) {
        DataCheckerConfigBundle result = new DataCheckerConfigBundle();
        String configFullDataCheckDirPath = taskWorkspace.getConfigFullDataCheckDirPath();
        result.setCheckConfigFile(new ConfigFile("application.yml", configFullDataCheckDirPath,
                taskWorkspace, TemplateConfigType.DATACHECKER_CHECK_CONFIG));
        result.setSinkConfigFile(new ConfigFile("application-sink.yml", configFullDataCheckDirPath,
                taskWorkspace, TemplateConfigType.DATACHECKER_SINK_CONFIG));
        result.setSourceConfigFile(new ConfigFile("application-source.yml", configFullDataCheckDirPath,
                taskWorkspace, TemplateConfigType.DATACHECKER_SOURCE_CONFIG));
        result.setLog4j2ConfigFile(new ConfigFile("log4j2.xml", configFullDataCheckDirPath,
                taskWorkspace, TemplateConfigType.DATACHECKER_LOG4J2_CONFIG));
        return result;
    }

    private DebeziumConfigBundle getIncrementalConfigBundle(TaskWorkspace taskWorkspace) {
        DebeziumConfigBundle result = new DebeziumConfigBundle();
        String configIncrementalDirPath = taskWorkspace.getConfigIncrementalDirPath();
        result.setConnectSinkConfigFile(new ConfigFile("incremental-connect-sink.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_MYSQL_SINK_CONFIG));
        result.setConnectSourceConfigFile(new ConfigFile("incremental-connect-source.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_MYSQL_SOURCE_CONFIG));
        result.setWorkerSinkConfigFile(new ConfigFile("incremental-worker-sink.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setWorkerSourceConfigFile(new ConfigFile("incremental-worker-source.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setLog4jSinkConfigFile(new ConfigFile("incremental-log4j-sink.properties", configIncrementalDirPath,
                taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
        result.setLog4jSourceConfigFile(new ConfigFile("incremental-log4j-source.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
        return result;
    }

    private DataCheckerConfigBundle getIncrementalDataCheckConfigBundle(TaskWorkspace taskWorkspace) {
        DataCheckerConfigBundle result = new DataCheckerConfigBundle();
        String configIncrementalDataCheckDirPath = taskWorkspace.getConfigIncrementalDataCheckDirPath();
        result.setCheckConfigFile(new ConfigFile("application.yml",
                configIncrementalDataCheckDirPath, taskWorkspace, TemplateConfigType.DATACHECKER_CHECK_CONFIG));
        result.setSinkConfigFile(new ConfigFile("application-sink.yml",
                configIncrementalDataCheckDirPath, taskWorkspace, TemplateConfigType.DATACHECKER_SINK_CONFIG));
        result.setSourceConfigFile(new ConfigFile("application-source.yml",
                configIncrementalDataCheckDirPath, taskWorkspace, TemplateConfigType.DATACHECKER_SOURCE_CONFIG));
        result.setLog4j2ConfigFile(new ConfigFile("log4j2.xml",
                configIncrementalDataCheckDirPath, taskWorkspace, TemplateConfigType.DATACHECKER_LOG4J2_CONFIG));
        return result;
    }

    private DebeziumConfigBundle getReverseConfigBundle(TaskWorkspace taskWorkspace) {
        DebeziumConfigBundle result = new DebeziumConfigBundle();
        String configReverseDirPath = taskWorkspace.getConfigReverseDirPath();
        result.setConnectSinkConfigFile(new ConfigFile("reverse-connect-sink.properties",
                configReverseDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_OPENGAUSS_SINK_CONFIG));
        result.setConnectSourceConfigFile(new ConfigFile("reverse-connect-source.properties",
                configReverseDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_OPENGAUSS_SOURCE_CONFIG));
        result.setWorkerSinkConfigFile(new ConfigFile("reverse-worker-sink.properties",
                configReverseDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setWorkerSourceConfigFile(new ConfigFile("reverse-worker-source.properties",
                configReverseDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setLog4jSinkConfigFile(new ConfigFile("reverse-log4j-sink.properties", configReverseDirPath,
                taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
        result.setLog4jSourceConfigFile(new ConfigFile("reverse-log4j-source.properties", configReverseDirPath,
                taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
        return result;
    }
}
