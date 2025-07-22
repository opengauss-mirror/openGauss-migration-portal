/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * migration phase
 *
 * @since 2025/2/27
 */
@Getter
@AllArgsConstructor
public enum MigrationPhase {
    FULL_MIGRATION("full_migration", "full migration phase"),
    FULL_DATA_CHECK("full_data_check", "full data check phase"),
    INCREMENTAL_MIGRATION("incremental_migration", "incremental migration phase"),
    INCREMENTAL_DATA_CHECK("incremental_data_check", "incremental data check phase"),
    REVERSE_MIGRATION("reverse_migration", "reverse migration phase")
    ;

    private final String phaseName;
    private final String phaseDesc;
}
