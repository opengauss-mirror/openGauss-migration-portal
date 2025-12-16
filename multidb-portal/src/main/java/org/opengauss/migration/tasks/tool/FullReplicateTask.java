/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.tool.FullReplicateConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.tool.config.FullReplicateConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.migration.process.task.FullReplicateProcess;
import org.opengauss.migration.process.task.TaskProcess;

/**
 * oG_datasync_full_migration task
 *
 * @since 2025/5/29
 */
public abstract class FullReplicateTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(FullReplicateTask.class);

    /**
     * Migration stop indicator
     */
    protected final MigrationStopIndicator migrationStopIndicator;

    /**
     * oG_datasync_full_migration config
     */
    protected final ConfigFile fullConfig;

    /**
     * JVM prefix
     */
    protected final String jvmPrefix;

    private TaskProcess currentProcess;

    protected FullReplicateTask(TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
                             FullReplicateConfigBundle fullMigrationToolConfig, String jvmPrefix) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.fullConfig = fullMigrationToolConfig.getConfigFile();
        this.jvmPrefix = jvmPrefix;
    }

    /**
     * Drop replica schema
     */
    protected void dropReplicaSchema() {
        FullReplicateProcess dropSchemaProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_DROP_REPLICA_SCHEMA, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                FullReplicateConstants.ORDER_DROP_REPLICA_SCHEMA,
                jvmPrefix);
        dropSchemaProcess.start();
        dropSchemaProcess.waitExit();
    }

    /**
     * Start migration table
     */
    protected void tableMigration() {
        FullReplicateProcess tableProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_TABLE, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_TABLE,
                jvmPrefix);

        currentProcess = tableProcess;
        tableProcess.start();
    }

    /**
     * Wait table migration exit
     */
    protected void waitTableMigrationExit() {
        if (currentProcess != null
                && currentProcess.getProcessName().equals(ProcessNameConstants.FULL_REPLICATE_MIGRATION_TABLE)) {
            currentProcess.waitExit();
            if (!currentProcess.checkStatus()) {
                throw new MigrationException("Failed to migrate table, please check the log for details.");
            }

            FullReplicateProcess sequenceProcess = new FullReplicateProcess(
                    ProcessNameConstants.FULL_REPLICATE_MIGRATION_SEQUENCE, taskWorkspace, fullConfig,
                    FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_SEQUENCE,
                    jvmPrefix);
            startProcessAndWaitExit(sequenceProcess);
            FullReplicateProcess primaryKeyProcess = new FullReplicateProcess(
                    ProcessNameConstants.FULL_REPLICATE_MIGRATION_PRIMARY_KEY, taskWorkspace, fullConfig,
                    FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    FullReplicateConstants.ORDER_PRIMARY_KEY, jvmPrefix);
            startProcessAndWaitExit(primaryKeyProcess);
            FullReplicateProcess indexProcess = new FullReplicateProcess(
                    ProcessNameConstants.FULL_REPLICATE_MIGRATION_INDEX, taskWorkspace, fullConfig,
                    FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_INDEX,
                    jvmPrefix);
            startProcessAndWaitExit(indexProcess);
            FullReplicateProcess constraintProcess = new FullReplicateProcess(
                    ProcessNameConstants.FULL_REPLICATE_MIGRATION_CONSTRAINT, taskWorkspace, fullConfig,
                    FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    FullReplicateConstants.ORDER_CONSTRAINT, jvmPrefix);
            startProcessAndWaitExit(constraintProcess);
        }
    }

    /**
     * Start migration trigger and wait exit
     */
    protected void triggerMigration() {
        FullReplicateProcess triggerProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_TRIGGER, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_TRIGGER,
                jvmPrefix);
        startProcessAndWaitExit(triggerProcess);
    }

    /**
     * Start migration view and wait exit
     */
    protected void viewMigration() {
        FullReplicateProcess viewProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_VIEW, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_VIEW,
                jvmPrefix);
        startProcessAndWaitExit(viewProcess);
    }

    /**
     * Start migration function and wait exit
     */
    protected void functionMigration() {
        FullReplicateProcess functionProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_FUNCTION, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_FUNCTION,
                jvmPrefix);
        startProcessAndWaitExit(functionProcess);
    }

    /**
     * Start migration procedure and wait exit
     */
    protected void procedureMigration() {
        FullReplicateProcess procedureProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_PROCEDURE, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_PROCEDURE,
                jvmPrefix);
        startProcessAndWaitExit(procedureProcess);
    }

    /**
     * Start migration foreign key and wait exit
     */
    protected void foreignKeyMigration() {
        FullReplicateProcess foreignKeyProcess = new FullReplicateProcess(
                ProcessNameConstants.FULL_REPLICATE_MIGRATION_FOREIGN_KEY, taskWorkspace, fullConfig,
                FullReplicateConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullReplicateConstants.ORDER_FOREIGN_KEY,
                jvmPrefix);
        startProcessAndWaitExit(foreignKeyProcess);
    }

    /**
     * Start migration
     */
    protected void afterMigration() {
        dropReplicaSchema();
    }

    /**
     * Stop current process
     */
    protected void stop() {
        if (currentProcess != null) {
            currentProcess.stop();
        }
    }

    private void startProcessAndWaitExit(TaskProcess process) {
        if (!migrationStopIndicator.isStopped()) {
            currentProcess = process;
            process.start();
            process.waitExit();
            if (!process.checkStatus()) {
                throw new MigrationException(
                        process.getProcessName() + " migration failed, please check the log for details.");
            }
        }
    }
}
