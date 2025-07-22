/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

import org.opengauss.enums.MigrationStatusEnum;

import java.util.List;

/**
 * migration status constants
 *
 * @since 2025/5/13
 */
public class MigrationStatusConstants {
    /**
     * migration status file name
     */
    public static final String MIGRATION_STATUS_FILE_NAME = "migration-status.txt";

    /**
     * full migration status file name: total.txt
     */
    public static final String FULL_TOTAL_INFO_STATUS_FILE_NAME = "total.txt";

    /**
     * full migration status file name: table.txt
     */
    public static final String FULL_TABLE_STATUS_FILE_NAME = "table.txt";

    /**
     * full migration status file name: trigger.txt
     */
    public static final String FULL_TRIGGER_STATUS_FILE_NAME = "trigger.txt";

    /**
     * full migration status file name: view.txt
     */
    public static final String FULL_VIEW_STATUS_FILE_NAME = "view.txt";

    /**
     * full migration status file name: function.txt
     */
    public static final String FULL_FUNCTION_STATUS_FILE_NAME = "function.txt";

    /**
     * full migration status file name: procedure.txt
     */
    public static final String FULL_PROCEDURE_STATUS_FILE_NAME = "procedure.txt";

    /**
     * full migration status file name: success.txt
     */
    public static final String FULL_CHECK_SUCCESS_OBJECT_STATUS_FILE_NAME = "success.txt";

    /**
     * full migration status file name: failed.txt
     */
    public static final String FULL_CHECK_FAILED_OBJECT_STATUS_FILE_NAME = "failed.txt";

    /**
     * incremental migration status file name
     */
    public static final String INCREMENTAL_STATUS_FILE_NAME = "incremental.txt";

    /**
     * reverse migration status file name
     */
    public static final String REVERSE_STATUS_FILE_NAME = "reverse.txt";

    /**
     * migration status in full phase list
     */
    public static final List<MigrationStatusEnum> MIGRATION_STATUS_IN_FULL_PHASE_LIST = List.of(
            MigrationStatusEnum.START_FULL_MIGRATION,
            MigrationStatusEnum.FULL_MIGRATION_RUNNING,
            MigrationStatusEnum.FULL_MIGRATION_FINISHED
    );

    /**
     * migration status in full check phase list
     */
    public static final List<MigrationStatusEnum> MIGRATION_STATUS_IN_FULL_CHECK_PHASE_LIST = List.of(
            MigrationStatusEnum.START_FULL_DATA_CHECK,
            MigrationStatusEnum.FULL_DATA_CHECK_RUNNING,
            MigrationStatusEnum.FULL_DATA_CHECK_FINISHED
    );

    /**
     * migration status in incremental phase list
     */
    public static final List<MigrationStatusEnum> MIGRATION_STATUS_IN_INCREMENTAL_PHASE_LIST = List.of(
            MigrationStatusEnum.START_INCREMENTAL_MIGRATION,
            MigrationStatusEnum.INCREMENTAL_MIGRATION_RUNNING,
            MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED,
            MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED
    );

    /**
     * migration status in reverse phase list
     */
    public static final List<MigrationStatusEnum> MIGRATION_STATUS_IN_REVERSE_PHASE_LIST = List.of(
            MigrationStatusEnum.START_REVERSE_MIGRATION,
            MigrationStatusEnum.REVERSE_MIGRATION_RUNNING,
            MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED,
            MigrationStatusEnum.REVERSE_MIGRATION_FINISHED
    );

    private MigrationStatusConstants() {
    }
}
