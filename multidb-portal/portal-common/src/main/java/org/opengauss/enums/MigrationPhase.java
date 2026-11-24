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
