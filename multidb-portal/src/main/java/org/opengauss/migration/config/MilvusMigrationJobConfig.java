/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.config;

import lombok.Getter;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.domain.tool.config.SingleConfigBundle;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.migration.helper.config.MilvusMigrationConfigHelper;

import java.util.List;
import java.util.Map;

/**
 * MilvusMigrationJobConfig
 *
 * @since 2025/10/20
 */
@Getter
public class MilvusMigrationJobConfig extends AbstractMigrationJobConfig {
    private final SingleConfigBundle fullConfigBundle;

    private volatile MilvusMigrationConfigDto migrationConfigDto;
    private Map<String, String> tableMappings;

    public MilvusMigrationJobConfig(TaskWorkspace taskWorkspace) {
        super(taskWorkspace, new ConfigFile("migration.properties", taskWorkspace.getConfigDirPath(),
                taskWorkspace, TemplateConfigType.MILVUS_MIGRATION_CONFIG));

        this.fullConfigBundle = getFullConfigBundle(taskWorkspace);
    }

    /**
     * Get migration config dto
     *
     * @return milvus migration config dto
     */
    public MilvusMigrationConfigDto getMigrationConfigDto() {
        if (migrationConfigDto == null) {
            throw new IllegalStateException("Milvus migration config is not loaded");
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
            throw new IllegalStateException("Milvus migration config is not loaded");
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
        migrationConfigDto = MilvusMigrationConfigDto.generateMilvusMigrationConfigDto(
                migrationConfigFile.getConfigMap());
        tableMappings = MilvusMigrationConfigHelper.milvusTableMappings(migrationConfigDto);
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
                MilvusMigrationConfigHelper.milvusFullMigrationConfig(migrationConfigDto, taskWorkspace));
    }

    private SingleConfigBundle getFullConfigBundle(TaskWorkspace taskWorkspace) {
        SingleConfigBundle result = new SingleConfigBundle();
        result.setConfigFile(new ConfigFile("config.ini", taskWorkspace.getConfigFullDirPath(), taskWorkspace,
                TemplateConfigType.MILVUS_MIGRATION_TOOL_CONFIG));
        return result;
    }
}
