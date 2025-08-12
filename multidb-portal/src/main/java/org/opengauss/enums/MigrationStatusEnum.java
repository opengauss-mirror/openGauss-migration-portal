/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.Getter;

/**
 * Migration status enum
 *
 * @since 2025/3/1
 */
@Getter
public enum MigrationStatusEnum {
    NOT_START(0, "Migration not started"),
    MIGRATION_STARTING(1, "Migration starting"),

    START_FULL_MIGRATION(100, "Full migration started"),
    FULL_MIGRATION_RUNNING(101, "Full migration running"),
    FULL_MIGRATION_FINISHED(102, "Full migration finished"),

    START_FULL_DATA_CHECK(200, "Full data check started"),
    FULL_DATA_CHECK_RUNNING(201, "Full data check running"),
    FULL_DATA_CHECK_FINISHED(202, "Full data check finished"),

    START_INCREMENTAL_MIGRATION(300, "Incremental migration started"),
    INCREMENTAL_MIGRATION_RUNNING(301, "Incremental migration running"),
    INCREMENTAL_MIGRATION_STOPPING(302, "Incremental migration stopping"),
    INCREMENTAL_MIGRATION_FINISHED(303, "Incremental migration finished"),

    START_REVERSE_MIGRATION(400, "Reverse migration started"),
    REVERSE_MIGRATION_RUNNING(401, "Reverse migration running"),
    REVERSE_MIGRATION_STOPPING(402, "Reverse migration stopping"),
    REVERSE_MIGRATION_FINISHED(403, "Reverse migration finished"),

    MIGRATION_FINISHED(600, "Migration finished"),
    MIGRATION_STOPPING(601, "Migration stopping"),

    MIGRATION_FAILED(500, "Migration failed"),
    INCREMENTAL_MIGRATION_INTERRUPTED(501, "Incremental migration interrupted"),
    REVERSE_MIGRATION_INTERRUPTED(502, "Reverse migration interrupted"),

    PRE_MIGRATION_VERIFY_FAILED(700, "Pre migration verify failed"),
    PRE_REVERSE_PHASE_VERIFY_FAILED(701, "Pre reverse phase verify failed"),
    ;

    MigrationStatusEnum(int status, String description) {
        this.status = status;
        this.description = description;
    }

    private final int status;
    private final String description;
}
