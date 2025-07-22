/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.mode;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengauss.enums.MigrationPhase;

import java.util.HashSet;
import java.util.List;

/**
 * Migration mode
 *
 * @since 2025/2/27
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MigrationMode {
    private String modeName;
    private List<MigrationPhase> migrationPhaseList;

    /**
     * Check if the migration mode contains the specified phase
     *
     * @param phase migration phase
     * @return true if the migration mode contains the specified phase, false otherwise
     */
    public boolean hasPhase(MigrationPhase phase) {
        return new HashSet<>(migrationPhaseList).contains(phase);
    }
}
