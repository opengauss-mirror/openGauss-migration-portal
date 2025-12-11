/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.config.ElasticsearchMigrationToolConfig;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.migration.config.ElasticsearchMigrationConfigDto;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ElasticsearchMigrationConfigHelper
 *
 * @since 2025/10/23
 */
public class ElasticsearchMigrationConfigHelper {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchMigrationConfigHelper.class);

    private ElasticsearchMigrationConfigHelper() {
    }

    /**
     * Get elasticsearch full migration config
     *
     * @param dto elasticsearch migration config dto
     * @return elasticsearch full migration config
     */
    public static Map<String, String> elasticsearchFullMigrationConfig(ElasticsearchMigrationConfigDto dto) {
        HashMap<String, String> changeParams = new HashMap<>();
        changeParams.put(ElasticsearchMigrationToolConfig.ELASTICSEARCH_HOST, dto.getElasticsearchHost());
        if (!dto.isElasticsearchAnonymousAccess()) {
            changeParams.put(ElasticsearchMigrationToolConfig.ELASTICSEARCH_USERNAME, dto.getElasticsearchUsername());
        }

        changeParams.put(ElasticsearchMigrationToolConfig.OPENGAUSS_IP, dto.getOpengaussDatabaseIp());
        changeParams.put(ElasticsearchMigrationToolConfig.OPENGAUSS_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(ElasticsearchMigrationToolConfig.OPENGAUSS_DATABASE, dto.getOpengaussDatabaseName());
        changeParams.put(ElasticsearchMigrationToolConfig.OPENGAUSS_USERNAME, dto.getOpengaussDatabaseUsername());

        return changeParams;
    }

    /**
     * Get elasticsearch index mappings
     *
     * @param dto elasticsearch migration config dto
     * @return elasticsearch index mappings
     */
    public static Map<String, String> elasticsearchTableMappings(ElasticsearchMigrationConfigDto dto) {
        String elasticsearchIndexes = dto.getElasticsearchIndexes();
        List<String> indexes = Arrays.asList(elasticsearchIndexes.split(","));

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

                String sourceIndex = parts[0];
                if (indexes.contains(sourceIndex)) {
                    resultMapping.put(sourceIndex, parts[1]);
                }
            }
        }

        for (String index : indexes) {
            if (!resultMapping.containsKey(index)) {
                resultMapping.put(index, index);
            }
        }
        return resultMapping;
    }
}
