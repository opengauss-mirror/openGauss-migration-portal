/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import lombok.Getter;
import org.opengauss.portalcontroller.constant.Status;

import java.util.HashMap;
import java.util.Map;

/**
 * migration phase of alert logs
 *
 * @since 2024/12/13
 */
@Getter
public enum AlertLogMigrationPhaseEnum {
    FULL_MIGRATION(1, "full migration phase"),
    FULL_MIGRATION_CHECK(2, "full migration data check phase"),
    INCREMENTAL_MIGRATION(3,
            "incremental migration and incremental migration data check phase"),
    REVERSE_MIGRATION(4, "reverse migration phase");

    private static final Map<Integer, Integer> STATUS_PHASE_MAP = new HashMap<>();

    private final int phaseId;
    private final String description;

    static {
        STATUS_PHASE_MAP.put(Status.START_FULL_MIGRATION, FULL_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.RUNNING_FULL_MIGRATION, FULL_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.FULL_MIGRATION_FINISHED, FULL_MIGRATION.phaseId);

        STATUS_PHASE_MAP.put(Status.START_FULL_MIGRATION_CHECK, FULL_MIGRATION_CHECK.phaseId);
        STATUS_PHASE_MAP.put(Status.RUNNING_FULL_MIGRATION_CHECK, FULL_MIGRATION_CHECK.phaseId);
        STATUS_PHASE_MAP.put(Status.FULL_MIGRATION_CHECK_FINISHED, FULL_MIGRATION_CHECK.phaseId);

        STATUS_PHASE_MAP.put(Status.START_INCREMENTAL_MIGRATION, INCREMENTAL_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.RUNNING_INCREMENTAL_MIGRATION, INCREMENTAL_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.INCREMENTAL_MIGRATION_FINISHED, INCREMENTAL_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.INCREMENTAL_MIGRATION_STOPPED, INCREMENTAL_MIGRATION.phaseId);

        STATUS_PHASE_MAP.put(Status.START_REVERSE_MIGRATION, REVERSE_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.RUNNING_REVERSE_MIGRATION, REVERSE_MIGRATION.phaseId);
        STATUS_PHASE_MAP.put(Status.REVERSE_MIGRATION_FINISHED, REVERSE_MIGRATION.phaseId);
    }

    AlertLogMigrationPhaseEnum(int phaseId, String description) {
        this.phaseId = phaseId;
        this.description = description;
    }

    /**
     * get migration phase id by migration process status
     *
     * @param status migration process status
     * @return int phase id
     */
    public static int getPhaseIdByStatus(int status) {
        return STATUS_PHASE_MAP.get(status);
    }
}
