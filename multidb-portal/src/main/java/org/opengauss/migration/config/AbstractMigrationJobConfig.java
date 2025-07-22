/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.config;

import lombok.Getter;
import org.opengauss.constants.config.MigrationConfig;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.migration.mode.MigrationMode;
import org.opengauss.migration.mode.ModeManager;

import java.util.List;

/**
 * Abstract migration job config
 *
 * @since 2025/7/2
 */
@Getter
public abstract class AbstractMigrationJobConfig {
    /**
     * Task workspace
     */
    protected final TaskWorkspace taskWorkspace;

    /**
     * Migration config file
     */
    protected final ConfigFile migrationConfigFile;

    private volatile List<MigrationPhase> migrationPhaseList;
    private volatile Boolean hasFullMigration;
    private volatile Boolean hasFullDataCheck;
    private volatile Boolean hasIncrementalMigration;
    private volatile Boolean hasIncrementalDataCheck;
    private volatile Boolean hasReverseMigration;

    AbstractMigrationJobConfig(TaskWorkspace taskWorkspace, ConfigFile migrationConfigFile) {
        this.taskWorkspace = taskWorkspace;
        this.migrationConfigFile = migrationConfigFile;
    }

    /**
     * Load migration phase list from migration.properties
     *
     * @return List migration phase list
     */
    public List<MigrationPhase> getMigrationPhaseList() {
        if (migrationPhaseList == null) {
            String modeName = migrationConfigFile.getConfigMap().get(MigrationConfig.MIGRATION_MODE).toString();
            MigrationMode migrationMode = new ModeManager().getModeByName(modeName);
            migrationPhaseList = migrationMode.getMigrationPhaseList();
        }
        return migrationPhaseList;
    }

    /**
     * Check whether migration phase list has full migration
     *
     * @return boolean has full migration
     */
    public boolean hasFullMigration() {
        if (migrationPhaseList == null || hasFullMigration == null) {
            hasFullMigration = getMigrationPhaseList().contains(MigrationPhase.FULL_MIGRATION);
        }
        return hasFullMigration;
    }

    /**
     * Check whether migration phase list has full data check
     *
     * @return boolean has full data check
     */
    public boolean hasFullDataCheck() {
        if (migrationPhaseList == null || hasFullDataCheck == null) {
            hasFullDataCheck = getMigrationPhaseList().contains(MigrationPhase.FULL_DATA_CHECK);
        }
        return hasFullDataCheck;
    }

    /**
     * Check whether migration phase list has incremental migration
     *
     * @return boolean has incremental migration
     */
    public boolean hasIncrementalMigration() {
        if (migrationPhaseList == null || hasIncrementalMigration == null) {
            hasIncrementalMigration = getMigrationPhaseList().contains(MigrationPhase.INCREMENTAL_MIGRATION);
        }
        return hasIncrementalMigration;
    }

    /**
     * Check whether migration phase list has incremental data check
     *
     * @return boolean has incremental data check
     */
    public boolean hasIncrementalDataCheck() {
        if (migrationPhaseList == null || hasIncrementalDataCheck == null) {
            hasIncrementalDataCheck = getMigrationPhaseList().contains(MigrationPhase.INCREMENTAL_DATA_CHECK);
        }
        return hasIncrementalDataCheck;
    }

    /**
     * Check whether migration phase list has reverse migration
     *
     * @return boolean has reverse migration
     */
    public boolean hasReverseMigration() {
        if (migrationPhaseList == null || hasReverseMigration == null) {
            hasReverseMigration = getMigrationPhaseList().contains(MigrationPhase.REVERSE_MIGRATION);
        }
        return hasReverseMigration;
    }

    /**
     * Load migration config from config files
     */
    public abstract void loadConfig();

    /**
     * Validate migration config
     */
    public abstract void validateConfig();

    /**
     * Change migration tools config
     */
    public abstract void changeToolsConfig();

    /**
     * Save change migration config
     */
    public abstract void saveChangeConfig();

    /**
     * Generate migration tools config files, when create task
     */
    public abstract void generateToolsConfigFiles();
}
