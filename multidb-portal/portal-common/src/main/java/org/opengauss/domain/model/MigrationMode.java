/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.domain.model;

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
