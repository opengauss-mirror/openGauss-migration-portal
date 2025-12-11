/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.job;

import org.opengauss.domain.migration.config.AbstractMigrationConfigDto;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tasks.phase.FullDataCheckTask;
import org.opengauss.migration.tasks.phase.FullMigrationTask;
import org.opengauss.migration.tasks.phase.IncrementalDataCheckTask;
import org.opengauss.migration.tasks.phase.IncrementalMigrationTask;
import org.opengauss.migration.tasks.phase.ReverseMigrationTask;
import org.opengauss.utils.JdbcUtils;
import org.opengauss.utils.OpenGaussUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Abstract migration job
 *
 * @since 2025/7/2
 */
public abstract class AbstractMigrationJob {
    /**
     * Has full migration
     */
    protected boolean hasFullMigration;

    /**
     * Has full data check
     */
    protected boolean hasFullDataCheck;

    /**
     * Has incremental migration
     */
    protected boolean hasIncrementalMigration;

    /**
     * Has incremental data check
     */
    protected boolean hasIncrementalDataCheck;

    /**
     * Has reverse migration
     */
    protected boolean hasReverseMigration;

    /**
     * Full migration task
     */
    protected FullMigrationTask fullMigrationTask;

    /**
     * Full data check task
     */
    protected FullDataCheckTask fullDataCheckTask;

    /**
     * Incremental migration task
     */
    protected IncrementalMigrationTask incrementalMigrationTask;

    /**
     * Incremental data check task
     */
    protected IncrementalDataCheckTask incrementalDataCheckTask;

    /**
     * Reverse migration task
     */
    protected ReverseMigrationTask reverseMigrationTask;

    /**
     * Pre migration verify
     *
     * @return true if pre-migration verify success, false otherwise
     */
    public abstract boolean preMigrationVerify();

    /**
     * Before migration
     */
    public abstract void beforeTask();

    /**
     * Start migration
     *
     * @param migrationStopIndicator migration stop indicator
     * @param processMonitor process monitor
     * @param statusMonitor status manager
     */
    public abstract void startTask(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor,
                                   StatusMonitor statusMonitor);

    /**
     * Stop incremental migration
     *
     * @param migrationStopIndicator migration stop indicator
     * @param statusMonitor status manager
     */
    public abstract void stopIncremental(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor);

    /**
     * Resume incremental migration
     *
     * @param statusMonitor status manager
     */
    public abstract void resumeIncremental(StatusMonitor statusMonitor);

    /**
     * Restart incremental migration
     *
     * @param migrationStopIndicator migration stop indicator
     * @param statusMonitor status manager
     */
    public abstract void restartIncremental(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor);

    /**
     * Start reverse migration
     *
     * @param migrationStopIndicator migration stop indicator
     * @param statusMonitor status manager
     */
    public abstract void startReverse(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor);

    /**
     * Stop reverse migration
     *
     * @param statusMonitor status manager
     */
    public abstract void stopReverse(StatusMonitor statusMonitor);

    /**
     * Resume reverse migration
     *
     * @param statusMonitor status manager
     */
    public abstract void resumeReverse(StatusMonitor statusMonitor);

    /**
     * Restart reverse migration
     *
     * @param migrationStopIndicator migration stop indicator
     * @param statusMonitor status manager
     */
    public abstract void restartReverse(MigrationStopIndicator migrationStopIndicator, StatusMonitor statusMonitor);

    /**
     * Stop migration
     */
    public abstract void stopTask();

    /**
     * Adjust kernel fsync param
     *
     * @param isOn whether fsync is on
     * @param migrationConfigDto migration config dto
     * @throws SQLException sql exception
     */
    protected void adjustKernelFsyncParam(boolean isOn, AbstractMigrationConfigDto migrationConfigDto)
            throws SQLException {
        if (!migrationConfigDto.getIsAdjustKernelParam().equalsIgnoreCase("true")) {
            return;
        }

        String fsyncParam = "fsync";
        String fsyncValue = isOn ? "on" : "off";
        try (Connection connection = JdbcUtils.getOpengaussConnection(migrationConfigDto.getOpenGaussConnectInfo())) {
            OpenGaussUtils.alterSystemSet(fsyncParam, fsyncValue, connection);
        }
    }

    abstract void generateTasks(MigrationStopIndicator migrationStopIndicator, ProcessMonitor processMonitor);

    abstract void afterTask();
}
