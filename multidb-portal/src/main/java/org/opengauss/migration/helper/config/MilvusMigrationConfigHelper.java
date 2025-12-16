/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.constants.config.MilvusMigrationToolConfig;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MilvusMigrationConfigHelper
 *
 * @since 2025/10/21
 */
public class MilvusMigrationConfigHelper {
    private static final Logger LOGGER = LogManager.getLogger(MilvusMigrationConfigHelper.class);

    private MilvusMigrationConfigHelper() {
    }

    /**
     * Generate milvus full migration config
     *
     * @param dto      milvus migration config dto
     * @param workspace task workspace
     * @return milvus full migration config
     */
    public static Map<String, String> milvusFullMigrationConfig(MilvusMigrationConfigDto dto, TaskWorkspace workspace) {
        HashMap<String, String> changeParams = new HashMap<>();
        changeParams.put(MilvusMigrationToolConfig.MILVUS_IP, dto.getMilvusIp());
        changeParams.put(MilvusMigrationToolConfig.MILVUS_PORT, dto.getMilvusPort());
        if (!dto.isMilvusAnonymousAccess()) {
            changeParams.put(MilvusMigrationToolConfig.MILVUS_USERNAME, dto.getMilvusUsername());
        }

        changeParams.put(MilvusMigrationToolConfig.OPENGAUSS_IP, dto.getOpengaussDatabaseIp());
        changeParams.put(MilvusMigrationToolConfig.OPENGAUSS_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(MilvusMigrationToolConfig.OPENGAUSS_DATABASE, dto.getOpengaussDatabaseName());
        changeParams.put(MilvusMigrationToolConfig.OPENGAUSS_USER, dto.getOpengaussDatabaseUsername());

        changeParams.put(MilvusMigrationToolConfig.OUTPUT_FOLDER, workspace.getTmpDirPath());
        changeParams.put(MilvusMigrationToolConfig.MIGRATION_CLEANUP_TEMP_FILES, "true");
        return changeParams;
    }

    /**
     * Generate milvus table mappings
     *
     * @param dto milvus migration config dto
     * @return Map table mappings
     */
    public static Map<String, String> milvusTableMappings(MilvusMigrationConfigDto dto) {
        String milvusCollections = dto.getMilvusCollections();
        List<String> collections = Arrays.asList(milvusCollections.split(","));

        String tableMappings = dto.getTableMappings();
        String[] configMappings = null;
        if (!StringUtils.isNullOrBlank(tableMappings)) {
            configMappings = tableMappings.split(",");
        }

        Map<String, String> resultMapping = new LinkedHashMap<>();
        if (configMappings != null) {
            for (String configMapping : configMappings) {
                if (StringUtils.isNullOrBlank(configMapping)) {
                    continue;
                }

                String[] parts = configMapping.split(":");
                if (parts.length != 2) {
                    LOGGER.error("Invalid table mapping: {}", configMapping);
                    throw new ConfigException("The " + MigrationConfig.TABLE_MAPPINGS + " is not a valid value");
                }

                String sourceCollection = parts[0];
                if (collections.contains(sourceCollection)) {
                    resultMapping.put(sourceCollection, parts[1]);
                }
            }
        }

        for (String collection : collections) {
            if (!resultMapping.containsKey(collection)) {
                resultMapping.put(collection, collection);
            }
        }
        return resultMapping;
    }
}
