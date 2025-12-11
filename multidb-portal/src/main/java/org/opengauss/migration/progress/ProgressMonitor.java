/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress;

import com.alibaba.fastjson2.JSON;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.migration.helper.tool.DebeziumHelper;
import org.opengauss.migration.handler.ThreadExceptionHandler;
import org.opengauss.migration.progress.model.FullEntry;
import org.opengauss.migration.progress.model.IncrementalAndReverseEntry;
import org.opengauss.migration.progress.model.tool.DebeziumSinkStatusEntry;
import org.opengauss.migration.progress.model.tool.DebeziumSourceStatusEntry;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.StringUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * progress monitor
 *
 * @since 2025/3/21
 */
public abstract class ProgressMonitor extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(ProgressMonitor.class);
    private static final int INTERVAL_TIME = 1000;

    /**
     * Status manager
     */
    protected final StatusMonitor statusMonitor;

    /**
     * Task workspace
     */
    protected final TaskWorkspace taskWorkspace;

    private final ConcurrentHashMap<String, Long> fileLastModifiedCache = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;
    private MigrationStatusEnum latestStatus = MigrationStatusEnum.NOT_START;

    ProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        super("Progress-Monitor-Thread");
        this.statusMonitor = statusMonitor;
        this.taskWorkspace = taskWorkspace;
    }

    abstract void readFullMigrationProgress();

    abstract void readFullDataCheckProgress();

    abstract void readIncrementalMigrationProgress();

    abstract void readReverseMigrationProgress();

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        while (isRunning) {
            ThreadUtils.sleep(INTERVAL_TIME);
            MigrationStatusEnum currentStatus = statusMonitor.getCurrentStatus().getStatus();
            if (MigrationStatusEnum.NOT_START.equals(currentStatus)
                    || MigrationStatusEnum.MIGRATION_STARTING.equals(currentStatus)) {
                continue;
            }
            if (MigrationStatusEnum.MIGRATION_FAILED.equals(currentStatus)
                    || MigrationStatusEnum.MIGRATION_FINISHED.equals(currentStatus)) {
                stopMonitoring();
                continue;
            }

            if (MigrationStatusEnum.MIGRATION_STOPPING.equals(currentStatus)) {
                currentStatus = latestStatus;
            }

            MigrationPhase currentPhase = getPhaseByStatus(currentStatus);
            readPhaseProgress(currentPhase);

            if (MigrationStatusEnum.NOT_START.equals(latestStatus)) {
                latestStatus = currentStatus;
                continue;
            }

            MigrationPhase latestPhase = getPhaseByStatus(latestStatus);
            if (!latestPhase.equals(currentPhase)) {
                readPhaseProgress(latestPhase);
            }

            latestStatus = currentStatus;
        }

        if (!MigrationStatusEnum.NOT_START.equals(latestStatus)) {
            readPhaseProgress(getPhaseByStatus(latestStatus));
        }
    }

    /**
     * Stop monitoring
     */
    public void stopMonitoring() {
        this.isRunning = false;

        if (!MigrationStatusEnum.NOT_START.equals(latestStatus)) {
            readPhaseProgress(getPhaseByStatus(latestStatus));
        }
    }

    /**
     * Read debezium incremental migration progress
     */
    protected void readDebeziumIncrementalMigrationProgress() {
        Optional<IncrementalAndReverseEntry> incrementalEntryOptional = readDebeziumStatusFileToEntry(false);
        if (incrementalEntryOptional.isEmpty()) {
            return;
        }

        try {
            String statusFilePath = MigrationStatusHelper.generateIncrementalStatusFilePath(taskWorkspace);
            FileUtils.writeToFile(statusFilePath, JSON.toJSONString(incrementalEntryOptional.get()), false);
        } catch (IOException e) {
            LOGGER.warn("Failed to write incremental migration progress, error: {}", e.getMessage());
        }
    }

    /**
     * Read debezium reverse migration progress.
     */
    protected void readDebeziumReverseMigrationProgress() {
        Optional<IncrementalAndReverseEntry> reverseEntryOptional = readDebeziumStatusFileToEntry(true);
        if (reverseEntryOptional.isEmpty()) {
            return;
        }

        try {
            String statusFilePath = MigrationStatusHelper.generateReverseStatusFilePath(taskWorkspace);
            FileUtils.writeToFile(statusFilePath, JSON.toJSONString(reverseEntryOptional.get()), false);
        } catch (IOException e) {
            LOGGER.warn("Failed to write reverse migration progress, error: {}", e.getMessage());
        }
    }

    /**
     * Is file modified
     *
     * @param filePath file path
     * @return boolean
     */
    protected boolean isFileModified(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return false;
        }

        long lastModified = file.lastModified();
        Long cacheModified = fileLastModifiedCache.get(filePath);
        if (cacheModified == null || lastModified != cacheModified) {
            fileLastModifiedCache.put(filePath, lastModified);
            return true;
        }

        return false;
    }

    /**
     * write object entry list
     *
     * @param entryList entry list
     * @param filePath file path
     */
    protected void writeObjectEntryList(List<FullEntry> entryList, String filePath) {
        try {
            if (entryList != null && !entryList.isEmpty()) {
                String jsonString = JSON.toJSONString(entryList);
                FileUtils.writeToFile(filePath, jsonString, false);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to write full migration progress, error: {}", e.getMessage());
        }
    }

    private Optional<IncrementalAndReverseEntry> readDebeziumStatusFileToEntry(boolean isReverse) {
        String sourceStatusFilePath;
        String sinkStatusFilePath;
        if (isReverse) {
            sourceStatusFilePath = DebeziumHelper.getReverseSourceStatusFilePath(taskWorkspace);
            sinkStatusFilePath = DebeziumHelper.getReverseSinkStatusFilePath(taskWorkspace);
        } else {
            sourceStatusFilePath = DebeziumHelper.getIncrementalSourceStatusFilePath(taskWorkspace);
            sinkStatusFilePath = DebeziumHelper.getIncrementalSinkStatusFilePath(taskWorkspace);
        }

        if (StringUtils.isNullOrBlank(sinkStatusFilePath) || StringUtils.isNullOrBlank(sourceStatusFilePath)
                || (!isFileModified(sourceStatusFilePath) && !isFileModified(sinkStatusFilePath))) {
            return Optional.empty();
        }
        Optional<DebeziumSourceStatusEntry> sourceStatusEntry =
                DebeziumHelper.parseDebeziumSourceStatusFile(sourceStatusFilePath);
        Optional<DebeziumSinkStatusEntry> sinkStatusEntry =
                DebeziumHelper.parseDebeziumSinkStatusFile(sinkStatusFilePath);
        if (sourceStatusEntry.isEmpty() || sinkStatusEntry.isEmpty()) {
            return Optional.empty();
        }

        DebeziumSourceStatusEntry sourceStatus = sourceStatusEntry.get();
        DebeziumSinkStatusEntry sinkStatus = sinkStatusEntry.get();
        IncrementalAndReverseEntry entry = new IncrementalAndReverseEntry();
        entry.setCount(sinkStatus.getReplayedCount() + sinkStatus.getOverallPipe());
        entry.setSourceSpeed(sourceStatus.getSpeed());
        entry.setSinkSpeed(sinkStatus.getSpeed());
        entry.setRest(sinkStatus.getOverallPipe());
        entry.setFailCount(sinkStatus.getFailCount());
        entry.setSuccessCount(sinkStatus.getSuccessCount());
        entry.setReplayedCount(sinkStatus.getReplayedCount());

        String failSqlFilePath;
        if (isReverse) {
            entry.setSkippedCount(sourceStatus.getSkippedExcludeCount());
            failSqlFilePath = DebeziumHelper.getDebeziumReverseFailSqlFilePath(taskWorkspace);
        } else {
            entry.setSkippedCount(sinkStatus.getSkippedCount() + sinkStatus.getSkippedExcludeEventCount());
            failSqlFilePath = DebeziumHelper.getDebeziumIncrementalFailSqlFilePath(taskWorkspace);
        }

        Path path = Path.of(failSqlFilePath);
        if (Files.exists(path)) {
            try {
                if (!StringUtils.isNullOrBlank(Files.readString(path))) {
                    entry.setHasFailSql(true);
                }
            } catch (IOException e) {
                LOGGER.trace("Failed to read fail sql file, error: {}", e.getMessage());
            }
        }
        return Optional.of(entry);
    }

    private MigrationPhase getPhaseByStatus(MigrationStatusEnum currentStatus) {
        if (MigrationStatusHelper.isFullMigrationStatus(currentStatus)) {
            return MigrationPhase.FULL_MIGRATION;
        }

        if (MigrationStatusHelper.isFullDataCheckStatus(currentStatus)) {
            return MigrationPhase.FULL_DATA_CHECK;
        }

        if (MigrationStatusHelper.isIncrementalMigrationStatus(currentStatus)) {
            return MigrationPhase.INCREMENTAL_MIGRATION;
        }

        if (MigrationStatusHelper.isReverseMigrationStatus(currentStatus)) {
            return MigrationPhase.REVERSE_MIGRATION;
        }
        throw new IllegalArgumentException("Invalid status: " + currentStatus);
    }

    private void readPhaseProgress(MigrationPhase phase) {
        if (MigrationPhase.FULL_MIGRATION.equals(phase)) {
            readFullMigrationProgress();
            return;
        }

        if (MigrationPhase.FULL_DATA_CHECK.equals(phase)) {
            readFullDataCheckProgress();
            return;
        }

        if (MigrationPhase.INCREMENTAL_MIGRATION.equals(phase)) {
            readIncrementalMigrationProgress();
            return;
        }

        if (MigrationPhase.REVERSE_MIGRATION.equals(phase)) {
            readReverseMigrationProgress();
        }
    }
}
