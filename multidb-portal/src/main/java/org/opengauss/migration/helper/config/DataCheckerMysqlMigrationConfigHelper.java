/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.opengauss.constants.config.DataCheckerCheckConfig;
import org.opengauss.constants.config.DataCheckerSinkConfig;
import org.opengauss.constants.config.DataCheckerSourceConfig;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * data checker mysql migration config helper
 *
 * @since 2025/5/8
 */
public class DataCheckerMysqlMigrationConfigHelper {
    private DataCheckerMysqlMigrationConfigHelper() {
    }

    /**
     * get mysql full data check source process config map
     *
     * @param dto mysql migration config dto
     * @param logConfigPath log config path
     * @return mysql full data check source config
     */
    public static Map<String, Object> mysqlFullDataCheckSourceConfig(
            MysqlMigrationConfigDto dto, String logConfigPath) {
        HashMap<String, Object> changeParams = new HashMap<>();

        String mysqlDatabaseIp = dto.getMysqlDatabaseIp();
        String mysqlDatabasePort = dto.getMysqlDatabasePort();
        String mysqlDatabaseName = dto.getMysqlDatabaseName();
        String mysqlDatabaseUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&useUnicode=true"
                        + "&characterEncoding=utf-8&serverTimezone=UTC&allowPublicKeyRetrieval=true",
                mysqlDatabaseIp, mysqlDatabasePort, mysqlDatabaseName);
        changeParams.put(DataCheckerSourceConfig.DATABASE_URL, mysqlDatabaseUrl);
        changeParams.put(DataCheckerSourceConfig.DATABASE_USERNAME, dto.getMysqlDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DataCheckerSourceConfig.DATABASE_PASSWORD, dto.getMysqlDatabasePassword());
        }
        changeParams.put(DataCheckerSourceConfig.EXTRACT_SCHEMA, mysqlDatabaseName);

        Kafka kafka = Kafka.getInstance();
        String kafkaIpPort = kafka.getKafkaIpPort();
        String schemaRegistryUrl = kafka.getSchemaRegistryUrl();
        changeParams.put(DataCheckerSourceConfig.EXTRACT_DEBEZIUM_AVRO_REGISTRY, schemaRegistryUrl);
        changeParams.put(DataCheckerSourceConfig.KAFKA_BOOTSTRAP_SERVERS, kafkaIpPort);

        changeParams.put(DataCheckerSourceConfig.EXTRACT_DEBEZIUM_ENABLE, false);
        changeParams.put(DataCheckerSourceConfig.LOGGING_CONFIG, logConfigPath);
        return changeParams;
    }

    /**
     * get mysql full data check sink process config map
     *
     * @param dto mysql migration config dto
     * @param logConfigPath log config path
     * @return mysql full data check sink config
     */
    public static Map<String, Object> mysqlFullDataCheckSinkConfig(MysqlMigrationConfigDto dto, String logConfigPath) {
        HashMap<String, Object> changeParams = new HashMap<>();

        String opengaussDatabaseIp = dto.getOpengaussDatabaseIp();
        String opengaussDatabasePort = dto.getOpengaussDatabasePort();
        String opengaussDatabaseName = dto.getOpengaussDatabaseName();
        String opengaussDatabaseUrl = String.format(
                "jdbc:opengauss://%s:%s/%s?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
                opengaussDatabaseIp, opengaussDatabasePort, opengaussDatabaseName);
        changeParams.put(DataCheckerSinkConfig.DATABASE_URL, opengaussDatabaseUrl);
        changeParams.put(DataCheckerSinkConfig.DATABASE_USERNAME, dto.getOpengaussDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(DataCheckerSinkConfig.DATABASE_PASSWORD, dto.getOpengaussDatabasePassword());
        }

        if (StringUtils.isNullOrBlank(dto.getOpengaussDatabaseSchema())) {
            changeParams.put(DataCheckerSinkConfig.EXTRACT_SCHEMA, dto.getMysqlDatabaseName());
        } else {
            changeParams.put(DataCheckerSinkConfig.EXTRACT_SCHEMA, dto.getOpengaussDatabaseSchema());
        }

        Kafka kafka = Kafka.getInstance();
        String kafkaIpPort = kafka.getKafkaIpPort();
        String schemaRegistryUrl = kafka.getSchemaRegistryUrl();
        changeParams.put(DataCheckerSinkConfig.EXTRACT_DEBEZIUM_AVRO_REGISTRY, schemaRegistryUrl);
        changeParams.put(DataCheckerSinkConfig.KAFKA_BOOTSTRAP_SERVERS, kafkaIpPort);

        changeParams.put(DataCheckerSinkConfig.EXTRACT_DEBEZIUM_ENABLE, false);
        changeParams.put(DataCheckerSinkConfig.LOGGING_CONFIG, logConfigPath);
        return changeParams;
    }

    /**
     * get mysql full data check the check process config map
     *
     * @param taskWorkspace task workspace
     * @param logConfigPath log config path
     * @return mysql full data check the check config
     */
    public static Map<String, Object> mysqlFullDataCheckCheckConfig(TaskWorkspace taskWorkspace, String logConfigPath) {
        HashMap<String, Object> changeParams = new HashMap<>();

        changeParams.put(DataCheckerCheckConfig.DATA_CHECK_DATA_PATH,
                DataCheckerHelper.generateFullDataCheckDataPath(taskWorkspace));
        changeParams.put(DataCheckerCheckConfig.LOGGING_CONFIG, logConfigPath);

        String kafkaIpPort = Kafka.getInstance().getKafkaIpPort();
        changeParams.put(DataCheckerCheckConfig.KAFKA_BOOTSTRAP_SERVERS, kafkaIpPort);
        return changeParams;
    }

    /**
     * get mysql incremental data check source process config map
     *
     * @param dto mysql migration config dto
     * @param logConfigPath log config path
     * @param sourceTopic incremental migration source topic
     * @return mysql incremental data check source config
     */
    public static Map<String, Object> mysqlIncrementalDataCheckSourceConfig(
            MysqlMigrationConfigDto dto, String logConfigPath, String sourceTopic) {
        Map<String, Object> changeParams = mysqlFullDataCheckSourceConfig(dto, logConfigPath);
        changeParams.put(DataCheckerSourceConfig.EXTRACT_DEBEZIUM_ENABLE, true);
        changeParams.put(DataCheckerSourceConfig.EXTRACT_DEBEZIUM_TOPIC, sourceTopic);
        return changeParams;
    }

    /**
     * get mysql incremental data check sink process config map
     *
     * @param dto mysql migration config dto
     * @param logConfigPath log config path
     * @param sinkTopic incremental migration sink topic
     * @return mysql incremental data check sink config
     */
    public static Map<String, Object> mysqlIncrementalDataCheckSinkConfig(
            MysqlMigrationConfigDto dto, String logConfigPath, String sinkTopic) {
        Map<String, Object> changeParams = mysqlFullDataCheckSinkConfig(dto, logConfigPath);
        changeParams.put(DataCheckerSinkConfig.EXTRACT_DEBEZIUM_ENABLE, true);
        changeParams.put(DataCheckerSinkConfig.EXTRACT_DEBEZIUM_TOPIC, sinkTopic);
        return changeParams;
    }

    /**
     * get mysql incremental data check the check process config map
     *
     * @param taskWorkspace task workspace
     * @param logConfigPath log config path
     * @return mysql incremental data check the check config
     */
    public static Map<String, Object> mysqlIncrementalDataCheckCheckConfig(
            TaskWorkspace taskWorkspace, String logConfigPath) {
        Map<String, Object> changeParams = mysqlFullDataCheckCheckConfig(taskWorkspace, logConfigPath);
        changeParams.put(DataCheckerCheckConfig.DATA_CHECK_DATA_PATH,
                DataCheckerHelper.generateIncrementalDataCheckDataPath(taskWorkspace));
        return changeParams;
    }
}
