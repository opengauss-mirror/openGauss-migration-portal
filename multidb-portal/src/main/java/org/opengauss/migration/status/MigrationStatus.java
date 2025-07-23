/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.status;

import lombok.Getter;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.utils.TimeUtils;

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
