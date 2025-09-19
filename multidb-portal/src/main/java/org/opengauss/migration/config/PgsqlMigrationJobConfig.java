/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.config;

import lombok.Getter;
import org.opengauss.constants.ConfigValidationConstants;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.dto.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.DebeziumConfigBundle;
import org.opengauss.domain.model.OgDatasyncConfigBundle;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DebeziumProcessType;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.migration.helper.config.DebeziumPgsqlMigrationConfigHelper;
import org.opengauss.migration.helper.config.OgDatasyncPgsqlMigrationConfigHelper;

import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL Migration Job Config
 *
 * @since 2025/7/2
 */
@Getter
public class PgsqlMigrationJobConfig extends AbstractMigrationJobConfig {
    private final OgDatasyncConfigBundle fullConfigBundle;
    private final DebeziumConfigBundle incrementalConfigBundle;
    private final DebeziumConfigBundle reverseConfigBundle;

    private volatile PgsqlMigrationConfigDto migrationConfigDto;

    public PgsqlMigrationJobConfig(TaskWorkspace taskWorkspace) {
        super(taskWorkspace, new ConfigFile("migration.properties", taskWorkspace.getConfigDirPath(),
                taskWorkspace, TemplateConfigType.PGSQL_MIGRATION_CONFIG));

        this.fullConfigBundle = getFullConfigBundle(taskWorkspace);
        this.incrementalConfigBundle = getIncrementalConfigBundle(taskWorkspace);
        this.reverseConfigBundle = getReverseConfigBundle(taskWorkspace);
    }

    /**
     * Get migration config dto.
     *
     * @return pgsql migration config dto
     */
    public PgsqlMigrationConfigDto getMigrationConfigDto() {
        if (migrationConfigDto == null) {
            throw new IllegalStateException("PostgreSQL migration config is not loaded");
        }
        return migrationConfigDto;
    }

    @Override
    public void loadConfig() {
        migrationConfigFile.loadConfigMap();
        migrationConfigDto = PgsqlMigrationConfigDto.generatePgsqlMigrationConfigDto(
                migrationConfigFile.getConfigMap());

        if (hasFullMigration()) {
            fullConfigBundle.loadConfigMap();
        }

        if (hasIncrementalMigration()) {
            incrementalConfigBundle.loadConfigMap();
        }

        if (hasReverseMigration()) {
            reverseConfigBundle.loadConfigMap();
        }
    }

    @Override
    public void validateConfig() {
        Map<String, Object> migrationConfig = migrationConfigFile.getConfigMap();
        String pgsqlIp = migrationConfig.get(MigrationConfig.PGSQL_DATABASE_IP).toString();
        String pgsqlPort = migrationConfig.get(MigrationConfig.PGSQL_DATABASE_PORT).toString();
        String opengaussIp = migrationConfig.get(MigrationConfig.OPENGAUSS_DATABASE_IP).toString();
        String opengaussPort = migrationConfig.get(MigrationConfig.OPENGAUSS_DATABASE_PORT).toString();

        if (!ConfigValidationConstants.IP_REGEX.matcher(pgsqlIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(pgsqlPort).matches()
                || !ConfigValidationConstants.IP_REGEX.matcher(opengaussIp).matches()
                || !ConfigValidationConstants.PORT_REGEX.matcher(opengaussPort).matches()) {
            throw new ConfigException("IP or Port is invalid");
        }
    }

    @Override
    public void changeToolsConfig() {
        if (hasFullMigration()) {
            changeFullConfig(hasIncrementalMigration());
        }

        if (hasIncrementalMigration()) {
            changeIncrementalConfig();
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

        if (hasIncrementalMigration()) {
            incrementalConfigBundle.saveConfigMap();
        }

        if (hasReverseMigration()) {
            reverseConfigBundle.saveConfigMap();
        }
    }

    @Override
    public void generateToolsConfigFiles() {
        migrationConfigFile.generateFile();
        fullConfigBundle.generateFile();
        incrementalConfigBundle.generateFile();
        reverseConfigBundle.generateFile();
    }

    private void changeFullConfig(boolean hasIncremental) {
        Map<String, Object> configMap = OgDatasyncPgsqlMigrationConfigHelper.pgsqlFullMigrationConfig(
                migrationConfigDto, taskWorkspace);
        if (hasIncremental) {
            configMap.putAll(OgDatasyncPgsqlMigrationConfigHelper.pgsqlFullMigrationRecordSnapshotConfig(
                    migrationConfigDto));
        }
        fullConfigBundle.getConfigFile().getConfigMap().putAll(configMap);
    }

    private void changeIncrementalConfig() {
        Map<String, Object> connectSourceParams = DebeziumPgsqlMigrationConfigHelper.incrementalSourceConfig(
                migrationConfigDto, taskWorkspace);
        incrementalConfigBundle.getConnectSourceConfigFile().getConfigMap().putAll(connectSourceParams);
        Set<String> sourceDeleteKeySet = DebeziumPgsqlMigrationConfigHelper.incrementalSourceConfigDeleteKeySet(
                migrationConfigDto);
        incrementalConfigBundle.getConnectSourceConfigFile().getDeleteConfigKeySet().addAll(sourceDeleteKeySet);

        Map<String, Object> connectSinkParams = DebeziumPgsqlMigrationConfigHelper.incrementalSinkConfig(
                migrationConfigDto, taskWorkspace);
        incrementalConfigBundle.getConnectSinkConfigFile().getConfigMap().putAll(connectSinkParams);
        Set<String> sinkDeleteKeySet = DebeziumPgsqlMigrationConfigHelper.incrementalSinkConfigDeleteKeySet(
                migrationConfigDto);
        incrementalConfigBundle.getConnectSinkConfigFile().getDeleteConfigKeySet().addAll(sinkDeleteKeySet);

        Map<String, Object> workerSourceParams = DebeziumPgsqlMigrationConfigHelper.incrementalWorkerSourceConfig(
                taskWorkspace);
        incrementalConfigBundle.getWorkerSourceConfigFile().getConfigMap().putAll(workerSourceParams);
        Map<String, Object> workerSinkParams = DebeziumPgsqlMigrationConfigHelper.incrementalWorkerSinkConfig(
                taskWorkspace);
        incrementalConfigBundle.getWorkerSinkConfigFile().getConfigMap().putAll(workerSinkParams);

        Map<String, Object> log4jSourceParams = DebeziumPgsqlMigrationConfigHelper.incrementalLog4jConfig(
                taskWorkspace, DebeziumProcessType.SOURCE);
        incrementalConfigBundle.getLog4jSourceConfigFile().getConfigMap().putAll(log4jSourceParams);
        Map<String, Object> log4jSinkParams = DebeziumPgsqlMigrationConfigHelper.incrementalLog4jConfig(
                taskWorkspace, DebeziumProcessType.SINK);
        incrementalConfigBundle.getLog4jSinkConfigFile().getConfigMap().putAll(log4jSinkParams);
    }

    private void changeReverseConfig() {
        Map<String, Object> connectSourceParams = DebeziumPgsqlMigrationConfigHelper.reverseSourceConfig(
                migrationConfigDto, taskWorkspace);
        reverseConfigBundle.getConnectSourceConfigFile().getConfigMap().putAll(connectSourceParams);
        Set<String> sourceDeleteKeySet = DebeziumPgsqlMigrationConfigHelper.reverseSourceConfigDeleteKeySet(
                migrationConfigDto);
        reverseConfigBundle.getConnectSourceConfigFile().getDeleteConfigKeySet().addAll(sourceDeleteKeySet);
        Map<String, Object> connectSinkParams = DebeziumPgsqlMigrationConfigHelper.reverseSinkConfig(
                migrationConfigDto, taskWorkspace);
        reverseConfigBundle.getConnectSinkConfigFile().getConfigMap().putAll(connectSinkParams);
        Set<String> sinkDeleteKeySet = DebeziumPgsqlMigrationConfigHelper.reverseSinkConfigDeleteKeySet(
                migrationConfigDto);
        reverseConfigBundle.getConnectSinkConfigFile().getDeleteConfigKeySet().addAll(sinkDeleteKeySet);

        Map<String, Object> workerSourceParams = DebeziumPgsqlMigrationConfigHelper.reverseWorkerSourceConfig(
                taskWorkspace);
        reverseConfigBundle.getWorkerSourceConfigFile().getConfigMap().putAll(workerSourceParams);
        Map<String, Object> workerSinkParams = DebeziumPgsqlMigrationConfigHelper.reverseWorkerSinkConfig(
                taskWorkspace);
        reverseConfigBundle.getWorkerSinkConfigFile().getConfigMap().putAll(workerSinkParams);

        Map<String, Object> log4jSourceParams = DebeziumPgsqlMigrationConfigHelper.reverseLog4jConfig(
                taskWorkspace, DebeziumProcessType.SOURCE);
        reverseConfigBundle.getLog4jSourceConfigFile().getConfigMap().putAll(log4jSourceParams);
        Map<String, Object> log4jSinkParams = DebeziumPgsqlMigrationConfigHelper.reverseLog4jConfig(
                taskWorkspace, DebeziumProcessType.SINK);
        reverseConfigBundle.getLog4jSinkConfigFile().getConfigMap().putAll(log4jSinkParams);
    }

    private OgDatasyncConfigBundle getFullConfigBundle(TaskWorkspace taskWorkspace) {
        OgDatasyncConfigBundle result = new OgDatasyncConfigBundle();
        result.setConfigFile(new ConfigFile("config.yml", taskWorkspace.getConfigFullDirPath(), taskWorkspace,
                TemplateConfigType.OG_DATASYNC_CONFIG));
        return result;
    }

    private DebeziumConfigBundle getIncrementalConfigBundle(TaskWorkspace taskWorkspace) {
        DebeziumConfigBundle result = new DebeziumConfigBundle();
        String configIncrementalDirPath = taskWorkspace.getConfigIncrementalDirPath();
        result.setConnectSinkConfigFile(new ConfigFile("incremental-connect-sink.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_PGSQL_SINK_CONFIG));
        result.setConnectSourceConfigFile(new ConfigFile("incremental-connect-source.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_PGSQL_SOURCE_CONFIG));
        result.setWorkerSinkConfigFile(new ConfigFile("incremental-worker-sink.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setWorkerSourceConfigFile(new ConfigFile("incremental-worker-source.properties",
                configIncrementalDirPath, taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG));
        result.setLog4jSinkConfigFile(new ConfigFile("incremental-log4j-sink.properties", configIncrementalDirPath,
                taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
        result.setLog4jSourceConfigFile(new ConfigFile("incremental-log4j-source.properties", configIncrementalDirPath,
                taskWorkspace, TemplateConfigType.DEBEZIUM_CONNECT_LOG4J2_CONFIG));
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
