/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.status;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.TaskException;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Migration status monitor
 *
 * @since 2025/3/1
 */
@Getter
public class StatusMonitor {
    private static final Logger LOGGER = LogManager.getLogger(StatusMonitor.class);

    private final List<MigrationStatus> statusList;
    private final TaskWorkspace taskWorkspace;

    private MigrationStatus currentStatus;

    public StatusMonitor(TaskWorkspace taskWorkspace) {
        this.taskWorkspace = taskWorkspace;
        this.currentStatus = new MigrationStatus(MigrationStatusEnum.NOT_START);
        this.statusList = new ArrayList<>();
        this.statusList.add(this.currentStatus);
    }

    /**
     * Set current status
     *
     * @param currentStatus current status
     */
    public void setCurrentStatus(MigrationStatusEnum currentStatus) {
        if (MigrationStatusEnum.MIGRATION_FAILED.equals(this.currentStatus.getStatus())) {
            return;
        }

        this.currentStatus = new MigrationStatus(currentStatus);
        this.statusList.add(this.currentStatus);
        LOGGER.info("Current status changed to: {}", currentStatus.getDescription());
        writeMigrationStatus(this.statusList, this.taskWorkspace);
    }

    /**
     * Whether current status is not running
     *
     * @return true if it is not running
     */
    public boolean isNotRunning() {
        MigrationStatusEnum status = currentStatus.getStatus();
        return MigrationStatusEnum.NOT_START.equals(status)
                || MigrationStatusEnum.MIGRATION_STARTING.equals(status)
                || MigrationStatusEnum.MIGRATION_STOPPING.equals(status)
                || MigrationStatusEnum.MIGRATION_FINISHED.equals(status)
                || MigrationStatusEnum.MIGRATION_FAILED.equals(status)
                || MigrationStatusEnum.PRE_MIGRATION_VERIFY_FAILED.equals(status);
    }

    /**
     * Whether current status is full migration status
     *
     * @return true if is full migration status
     */
    public boolean isFullMigrationStatus() {
        return MigrationStatusHelper.isFullMigrationStatus(currentStatus.getStatus());
    }

    /**
     * Whether current status is full data check status
     *
     * @return true if is full data check status
     */
    public boolean isFullDataCheckStatus() {
        return MigrationStatusHelper.isFullDataCheckStatus(currentStatus.getStatus());
    }

    /**
     * Whether current status is incremental migration status
     *
     * @return true if is incremental migration status
     */
    public boolean isIncrementalMigrationStatus() {
        return MigrationStatusHelper.isIncrementalMigrationStatus(currentStatus.getStatus());
    }

    /**
     * Whether current status is incremental migration stopped status
     *
     * @return true if is incremental migration stopped status
     */
    public boolean isIncrementalMigrationStopped() {
        return MigrationStatusEnum.INCREMENTAL_MIGRATION_FINISHED.equals(currentStatus.getStatus());
    }

    /**
     * Whether current status is reverse migration status
     *
     * @return true if is reverse migration status
     */
    public boolean isReverseMigrationStatus() {
        return MigrationStatusHelper.isReverseMigrationStatus(currentStatus.getStatus());
    }

    /**
     * Whether current status is reverse migration stopped status
     *
     * @return true if is reverse migration stopped status
     */
    public boolean isReverseMigrationStopped() {
        return MigrationStatusEnum.REVERSE_MIGRATION_FINISHED.equals(currentStatus.getStatus());
    }

    private void writeMigrationStatus(List<MigrationStatus> statusList, TaskWorkspace taskWorkspace) {
        try {
            String jsonString = JSON.toJSONString(statusList, JSONWriter.Feature.PrettyFormat);
            String statusFilePath = MigrationStatusHelper.generateMigrationStatusFilePath(taskWorkspace);
            FileUtils.writeToFile(statusFilePath, jsonString, false);
        } catch (IOException e) {
            throw new TaskException("Failed to write migration status list to file", e);
        }
    }
}
