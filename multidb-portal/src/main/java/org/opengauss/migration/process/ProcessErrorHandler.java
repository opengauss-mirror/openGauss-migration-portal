/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.KafkaException;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.MigrationManager;
import org.opengauss.migration.process.task.DataCheckerProcess;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tools.Kafka;

/**
 * process error handler
 *
 * @since 2025/6/6
 */
public class ProcessErrorHandler {
    private static final Logger LOGGER = LogManager.getLogger(ProcessErrorHandler.class);

    private final MigrationManager migrationManager;
    private final StatusMonitor statusMonitor;

    public ProcessErrorHandler(MigrationManager migrationManager, StatusMonitor statusMonitor) {
        this.migrationManager = migrationManager;
        this.statusMonitor = statusMonitor;
    }

    /**
     * handle task process error
     *
     * @param process task process
     */
    public void handleTaskProcessError(TaskProcess process) {
        if (process instanceof DataCheckerProcess) {
            throw new MigrationException("Data checker process has exit abnormally, stop migration");
        }

        if (process instanceof DebeziumProcess) {
            if (statusMonitor.isIncrementalMigrationStatus()) {
                LOGGER.error("Debezium process is abnormal, interrupt incremental migration");
                statusMonitor.setCurrentStatus(MigrationStatusEnum.INCREMENTAL_MIGRATION_INTERRUPTED);
            }

            if (statusMonitor.isReverseMigrationStatus()) {
                LOGGER.error("Debezium process is abnormal, interrupt reverse migration");
                statusMonitor.setCurrentStatus(MigrationStatusEnum.REVERSE_MIGRATION_INTERRUPTED);
            }
        }
    }

    /**
     * handle confluent process error
     */
    public void handleConfluentError() {
        if (statusMonitor.isNotRunning() || statusMonitor.isFullMigrationStatus()) {
            return;
        }

        if (statusMonitor.isFullDataCheckStatus()) {
            throw new KafkaException("Kafka process has exit abnormally");
        }

        boolean isRestarted = Kafka.getInstance().restart();
        if (statusMonitor.isIncrementalMigrationStatus()) {
            if (isRestarted) {
                if (!statusMonitor.isIncrementalMigrationStopped()) {
                    LOGGER.info("Restarted Kafka process successfully, restarting incremental migration...");
                    migrationManager.restartIncremental();
                }
                return;
            }
            throw new MigrationException("During the incremental migration process, Kafka process abnormally exited");
        }

        if (statusMonitor.isReverseMigrationStatus()) {
            if (isRestarted) {
                if (!statusMonitor.isReverseMigrationStopped()) {
                    LOGGER.info("Restarted Kafka process successfully, restarting reverse migration...");
                    migrationManager.restartReverse();
                }
            } else {
                throw new MigrationException("During the reverse migration process, Kafka process abnormally exited");
            }
        }
    }
}
