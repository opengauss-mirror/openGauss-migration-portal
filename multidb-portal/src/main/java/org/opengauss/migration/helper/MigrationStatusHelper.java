/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper;

import org.opengauss.constants.MigrationStatusConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationStatusEnum;

/**
 * migration status helper
 *
 * @since 2025/5/13
 */
public class MigrationStatusHelper {
    private MigrationStatusHelper() {
    }

    /**
     * generate migration status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateMigrationStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusDirPath();
        return String.format("%s/%s", statusDirPath, MigrationStatusConstants.MIGRATION_STATUS_FILE_NAME);
    }

    /**
     * generate full migration total info status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullTotalInfoStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_TOTAL_INFO_STATUS_FILE_NAME);
    }

    /**
     * generate full migration table status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullTableStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_TABLE_STATUS_FILE_NAME);
    }

    /**
     * generate full migration success table status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullSuccessTableStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_SUCCESS_TABLE_STATUS_FILE_NAME);
    }

    /**
     * generate full migration failed table status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullFailedTableStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_FAILED_TABLE_STATUS_FILE_NAME);
    }

    /**
     * generate full migration trigger status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullTriggerStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_TRIGGER_STATUS_FILE_NAME);
    }

    /**
     * generate full migration view status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullViewStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_VIEW_STATUS_FILE_NAME);
    }

    /**
     * generate full migration function status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullFuncStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_FUNCTION_STATUS_FILE_NAME);
    }

    /**
     * generate full migration procedure status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullProcStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusFullDirPath = taskWorkspace.getStatusFullDirPath();
        return String.format("%s/%s", statusFullDirPath, MigrationStatusConstants.FULL_PROCEDURE_STATUS_FILE_NAME);
    }

    /**
     * generate full migration check success object status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullCheckSuccessObjectStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDir = taskWorkspace.getStatusFullDataCheckDirPath();
        return String.format("%s/%s", statusDir, MigrationStatusConstants.FULL_CHECK_SUCCESS_OBJECT_STATUS_FILE_NAME);
    }

    /**
     * generate full migration check failed object status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateFullCheckFailedObjectStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDir = taskWorkspace.getStatusFullDataCheckDirPath();
        return String.format("%s/%s", statusDir, MigrationStatusConstants.FULL_CHECK_FAILED_OBJECT_STATUS_FILE_NAME);
    }

    /**
     * generate full migration status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateIncrementalStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        return String.format("%s/%s", statusDirPath, MigrationStatusConstants.INCREMENTAL_STATUS_FILE_NAME);
    }

    /**
     * generate incremental migration status file path
     *
     * @param taskWorkspace task workspace
     * @return String file path
     */
    public static String generateReverseStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        return String.format("%s/%s", statusDirPath, MigrationStatusConstants.REVERSE_STATUS_FILE_NAME);
    }

    /**
     * Is full migration status
     *
     * @param status migration status enum
     * @return boolean
     */
    public static boolean isFullMigrationStatus(MigrationStatusEnum status) {
        return MigrationStatusConstants.MIGRATION_STATUS_IN_FULL_PHASE_LIST.contains(status);
    }

    /**
     * Is full data check status
     *
     * @param status migration status enum
     * @return boolean
     */
    public static boolean isFullDataCheckStatus(MigrationStatusEnum status) {
        return MigrationStatusConstants.MIGRATION_STATUS_IN_FULL_CHECK_PHASE_LIST.contains(status);
    }

    /**
     * Is incremental migration status
     *
     * @param status migration status enum
     * @return boolean
     */
    public static boolean isIncrementalMigrationStatus(MigrationStatusEnum status) {
        return MigrationStatusConstants.MIGRATION_STATUS_IN_INCREMENTAL_PHASE_LIST.contains(status);
    }

    /**
     * Is reverse migration status
     *
     * @param status migration status enum
     * @return boolean
     */
    public static boolean isReverseMigrationStatus(MigrationStatusEnum status) {
        return MigrationStatusConstants.MIGRATION_STATUS_IN_REVERSE_PHASE_LIST.contains(status);
    }
}
