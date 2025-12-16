/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.FullReplicateConfig;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.PgsqlUtils;
import org.opengauss.utils.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * oG_datasync_full_migration pgsql migration config helper
 *
 * @since 2025/5/29
 */
public class FullReplicatePgsqlMigrationConfigHelper {
    private static final Logger LOGGER = LogManager.getLogger(FullReplicatePgsqlMigrationConfigHelper.class);
    private static final String PLUGIN_PGOUTPUT = "pgoutput";
    private static final String PLUGIN_WAL2JSON = "wal2json";

    private static int pgsqlMajorVersion = 0;

    private FullReplicatePgsqlMigrationConfigHelper() {
    }

    /**
     * get pgsql full migration config map
     *
     * @param dto pgsql migration config dto
     * @param workspace task workspace
     * @return change params
     */
    public static Map<String, Object> pgsqlFullMigrationConfig(PgsqlMigrationConfigDto dto, TaskWorkspace workspace) {
        HashMap<String, Object> changeParams = new HashMap<>();

        changeParams.put(FullReplicateConfig.IS_DUMP_JSON, true);
        changeParams.put(FullReplicateConfig.STATUS_DIR, workspace.getStatusFullDirPath());

        changeParams.put(FullReplicateConfig.OG_CONN_HOST, dto.getOpengaussDatabaseIp());
        changeParams.put(FullReplicateConfig.OG_CONN_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(FullReplicateConfig.OG_CONN_USER, dto.getOpengaussDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(FullReplicateConfig.OG_CONN_PASSWORD, dto.getOpengaussDatabasePassword());
        }
        changeParams.put(FullReplicateConfig.OG_CONN_DATABASE, dto.getOpengaussDatabaseName());

        changeParams.put(FullReplicateConfig.SOURCE_DB_CONN_HOST, dto.getPgsqlDatabaseIp());
        changeParams.put(FullReplicateConfig.SOURCE_DB_CONN_PORT, dto.getPgsqlDatabasePort());
        changeParams.put(FullReplicateConfig.SOURCE_DB_CONN_USER, dto.getPgsqlDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(FullReplicateConfig.SOURCE_DB_CONN_PASSWORD, dto.getPgsqlDatabasePassword());
        }
        changeParams.put(FullReplicateConfig.SOURCE_DB_CONN_DATABASE, dto.getPgsqlDatabaseName());

        changeParams.put(FullReplicateConfig.SOURCE_SCHEMA_MAPPINGS, getMigrationSchemaMappings(dto));
        changeParams.put(FullReplicateConfig.IS_DELETE_CSV, false);
        changeParams.put(FullReplicateConfig.SOURCE_CSV_DIR, generateCsvDirPath(workspace));
        changeParams.put(FullReplicateConfig.IS_RECORD_SNAPSHOT, false);
        return changeParams;
    }

    /**
     * get pgsql full migration record snapshot config map
     *
     * @param dto pgsql migration config dto
     * @return change params
     */
    public static Map<String, Object> pgsqlFullMigrationRecordSnapshotConfig(PgsqlMigrationConfigDto dto) {
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(FullReplicateConfig.IS_RECORD_SNAPSHOT, true);
        int majorPgsqlVersion = getMajorPgsqlVersion(dto);
        if (majorPgsqlVersion >= 10) {
            changeParams.put(FullReplicateConfig.PLUGIN_NAME, PLUGIN_PGOUTPUT);
        } else {
            changeParams.put(FullReplicateConfig.PLUGIN_NAME, PLUGIN_WAL2JSON);
        }
        return changeParams;
    }

    /**
     * get major pgsql version
     *
     * @param dto pgsql migration config dto
     * @return int major pgsql version
     */
    public static int getMajorPgsqlVersion(PgsqlMigrationConfigDto dto) {
        if (pgsqlMajorVersion != 0) {
            return pgsqlMajorVersion;
        }

        try (Connection connection = JdbcUtils.getPgsqlConnection(dto.getPgsqlConnectInfo())) {
            String pgsqlVersion = PgsqlUtils.getPgsqlVersion(connection);
            if (pgsqlVersion != null) {
                String[] versionParts = pgsqlVersion.split("\\.");
                if (versionParts.length >= 2) {
                    pgsqlMajorVersion = Integer.parseInt(versionParts[0]);
                    return pgsqlMajorVersion;
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConfigException("Failed to get pgsql version", e);
        }
        throw new ConfigException("Failed to parse pgsql version");
    }

    /**
     * generate csv dir path
     *
     * @param taskWorkspace task workspace
     * @return String csv dir path
     */
    public static String generateCsvDirPath(TaskWorkspace taskWorkspace) {
        return String.format("%s/csv", taskWorkspace.getTmpDirPath());
    }

    /**
     * get migration schema mappings
     *
     * @param dto pgsql migration config dto
     * @return Map schema mappings
     */
    public static Map<String, String> getMigrationSchemaMappings(PgsqlMigrationConfigDto dto) {
        String pgsqlDatabaseSchemas = dto.getPgsqlDatabaseSchemas();
        List<String> pgSchemas = Arrays.asList(pgsqlDatabaseSchemas.split(","));

        String schemaMappings = dto.getSchemaMappings();
        String[] configMappings = null;
        if (!StringUtils.isNullOrBlank(schemaMappings)) {
            configMappings = schemaMappings.split(",");
        }

        Map<String, String> resultMapping = new HashMap<>();
        if (configMappings != null) {
            for (String configMapping : configMappings) {
                if (StringUtils.isNullOrBlank(configMapping)) {
                    continue;
                }

                String[] parts = configMapping.split(":");
                if (parts.length != 2) {
                    LOGGER.error("Invalid schema mapping: {}", configMapping);
                    throw new ConfigException("The " + MigrationConfig.SCHEMA_MAPPINGS + " is not a valid value");
                }

                String sourceSchema = parts[0];
                if (pgSchemas.contains(sourceSchema)) {
                    resultMapping.put(sourceSchema, parts[1]);
                }
            }
        }

        for (String configSchema : pgSchemas) {
            if (!resultMapping.containsKey(configSchema)) {
                resultMapping.put(configSchema, configSchema);
            }
        }
        return resultMapping;
    }
}
