/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

import org.opengauss.enums.MigrationPhase;
import org.opengauss.migration.mode.MigrationMode;

import java.util.List;

/**
 * migration mode constants
 *
 * @since 2025/4/22
 */
public class MigrationModeConstants {
    /**
     * custom mode storage file name
     */
    public static final String CUSTOM_MODE_STORAGE_FILE_NAME = "migration-mode.txt";

    /**
     * object separator
     */
    public static final String OBJECT_SEPARATOR = "<<<END_OF_OBJECT>>>";

    /**
     * define mode template name
     */
    public static final String DEFINE_MODE_TEMPLATE_NAME = "mode-template.properties";

    /**
     * define mode template resources path
     */
    public static final String DEFINE_MODE_TEMPLATE_RESOURCES_PATH = "mode/" + DEFINE_MODE_TEMPLATE_NAME;

    /**
     * template key: mode name
     */
    public static final String TEMPLATE_KEY_MODE_NAME = "mode.name";

    /**
     * template key: migration phase list
     */
    public static final String TEMPLATE_KEY_MIGRATION_PHASE_LIST = "migration.phases";

    /**
     * mode name max length
     */
    public static final int MODE_NAME_MAX_LENGTH = 50;

    /**
     * mode name pattern
     */
    public static final String MODE_NAME_PATTERN = "^[a-zA-Z0-9_-]+$";

    /**
     * default mode list
     */
    public static final List<MigrationMode> DEFALUT_MODE_LIST = List.of(
            new MigrationMode("plan1",
                    List.of(MigrationPhase.FULL_MIGRATION, MigrationPhase.FULL_DATA_CHECK)
            ),
            new MigrationMode("plan2",
                    List.of(MigrationPhase.FULL_MIGRATION, MigrationPhase.FULL_DATA_CHECK,
                            MigrationPhase.INCREMENTAL_MIGRATION, MigrationPhase.INCREMENTAL_DATA_CHECK)
            ),
            new MigrationMode("plan3",
                    List.of(MigrationPhase.FULL_MIGRATION, MigrationPhase.FULL_DATA_CHECK,
                            MigrationPhase.INCREMENTAL_MIGRATION, MigrationPhase.INCREMENTAL_DATA_CHECK,
                            MigrationPhase.REVERSE_MIGRATION)
            ),
            new MigrationMode(MigrationPhase.FULL_MIGRATION.getPhaseName(), List.of(MigrationPhase.FULL_MIGRATION)),
            new MigrationMode(MigrationPhase.FULL_DATA_CHECK.getPhaseName(), List.of(MigrationPhase.FULL_DATA_CHECK)),
            new MigrationMode(MigrationPhase.INCREMENTAL_MIGRATION.getPhaseName(),
                    List.of(MigrationPhase.INCREMENTAL_MIGRATION)),
            new MigrationMode(MigrationPhase.REVERSE_MIGRATION.getPhaseName(),
                    List.of(MigrationPhase.REVERSE_MIGRATION))
    );

    private MigrationModeConstants() {
    }
}
