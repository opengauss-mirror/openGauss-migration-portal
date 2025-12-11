/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.config;

import lombok.Getter;
import org.opengauss.domain.migration.config.ElasticsearchMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.domain.tool.config.SingleConfigBundle;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.migration.helper.config.ElasticsearchMigrationConfigHelper;

import java.util.List;
import java.util.Map;

/**
 * ElasticsearchMigrationJobConfig
 *
 * @since 2025/10/21
 */
@Getter
public class ElasticsearchMigrationJobConfig extends AbstractMigrationJobConfig {
    private final SingleConfigBundle fullConfigBundle;

    private volatile ElasticsearchMigrationConfigDto migrationConfigDto;
    private Map<String, String> tableMappings;

    public ElasticsearchMigrationJobConfig(TaskWorkspace taskWorkspace) {
        super(taskWorkspace, new ConfigFile("migration.properties", taskWorkspace.getConfigDirPath(),
                taskWorkspace, TemplateConfigType.ELASTICSEARCH_MIGRATION_CONFIG));

        this.fullConfigBundle = getFullConfigBundle(taskWorkspace);
    }

    /**
     * Get migration config dto
     *
     * @return elasticsearch migration config dto
     */
    public ElasticsearchMigrationConfigDto getMigrationConfigDto() {
        if (migrationConfigDto == null) {
            throw new IllegalStateException("Elasticsearch migration config is not loaded");
        }
        return migrationConfigDto;
    }

    /**
     * Get table mappings
     *
     * @return table mappings
     */
    public Map<String, String> getTableMappings() {
        if (tableMappings == null) {
            throw new IllegalStateException("Elasticsearch migration config is not loaded");
        }
        return tableMappings;
    }

    @Override
    public List<MigrationPhase> getMigrationPhaseList() {
        return List.of(MigrationPhase.FULL_MIGRATION);
    }

    @Override
    public void loadConfig() {
        migrationConfigFile.loadConfigMap();
        migrationConfigDto = ElasticsearchMigrationConfigDto.generateElasticsearchMigrationConfigDto(
                migrationConfigFile.getConfigMap());
        tableMappings = ElasticsearchMigrationConfigHelper.elasticsearchTableMappings(migrationConfigDto);
        fullConfigBundle.loadConfigMap();
    }

    @Override
    public void changeToolsConfig() {
        changeFullConfig();
    }

    @Override
    public void saveChangeConfig() {
        fullConfigBundle.saveConfigMap();
    }

    @Override
    public void generateToolsConfigFiles() {
        migrationConfigFile.generateFile();
        fullConfigBundle.generateFile();
    }

    private void changeFullConfig() {
        fullConfigBundle.getConfigFile().getConfigMap().putAll(
                ElasticsearchMigrationConfigHelper.elasticsearchFullMigrationConfig(migrationConfigDto));
    }

    private SingleConfigBundle getFullConfigBundle(TaskWorkspace taskWorkspace) {
        SingleConfigBundle result = new SingleConfigBundle();
        result.setConfigFile(new ConfigFile("config.ini", taskWorkspace.getConfigFullDirPath(), taskWorkspace,
                TemplateConfigType.ELASTICSEARCH_MIGRATION_TOOL_CONFIG));
        return result;
    }
}
