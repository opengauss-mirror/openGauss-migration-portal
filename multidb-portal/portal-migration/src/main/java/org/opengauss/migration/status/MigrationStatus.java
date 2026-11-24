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

package org.opengauss.migration.status;

import lombok.Getter;
import org.opengauss.migration.enums.MigrationStatusEnum;
import org.opengauss.migration.utils.TimeUtils;

/**
 * migration status
 *
 * @since 2025/5/12
 */
@Getter
public class MigrationStatus {
    private final long timestamp;
    private final MigrationStatusEnum status;

    public MigrationStatus(MigrationStatusEnum status) {
        this.timestamp = TimeUtils.getCurrentTimeMillis();
        this.status = status;
    }
}
