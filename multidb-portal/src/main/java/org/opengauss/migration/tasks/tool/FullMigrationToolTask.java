/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.tool.FullMigrationToolConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.FullMigrationToolConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.migration.process.task.FullMigrationToolProcess;
import org.opengauss.migration.process.task.TaskProcess;

/**
 * Full migration tool task
 *
 * @since 2025/5/29
 */
public abstract class FullMigrationToolTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(FullMigrationToolTask.class);

    /**
     * Migration stop indicator
     */
    protected final MigrationStopIndicator migrationStopIndicator;

    /**
     * Full migration tool config
     */
    protected final ConfigFile fullConfig;

    /**
     * JVM prefix
     */
    protected final String jvmPrefix;

    private TaskProcess currentProcess;

    protected FullMigrationToolTask(TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
                                    FullMigrationToolConfigBundle fullMigrationToolConfig, String jvmPrefix) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.fullConfig = fullMigrationToolConfig.getConfigFile();
        this.jvmPrefix = jvmPrefix;
    }

    /**
     * Drop replica schema
     */
    protected void dropReplicaSchema() {
        FullMigrationToolProcess dropSchemaProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_DROP_REPLICA_SCHEMA, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                FullMigrationToolConstants.ORDER_DROP_REPLICA_SCHEMA,
                jvmPrefix);
        dropSchemaProcess.start();
        dropSchemaProcess.waitExit();
    }

    /**
     * Start migration table
     */
    protected void tableMigration() {
        FullMigrationToolProcess tableProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_TABLE, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_TABLE,
                jvmPrefix);

        currentProcess = tableProcess;
        tableProcess.start();
    }

    /**
     * Wait table migration exit
     */
    protected void waitTableMigrationExit() {
        if (currentProcess != null
                && currentProcess.getProcessName().equals(ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_TABLE)) {
            currentProcess.waitExit();
            if (!currentProcess.checkStatus()) {
                throw new MigrationException("Failed to migrate table, please check the log for details.");
            }

            FullMigrationToolProcess sequenceProcess = new FullMigrationToolProcess(
                    ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_SEQUENCE, taskWorkspace, fullConfig,
                    FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_SEQUENCE,
                    jvmPrefix);
            startProcessAndWaitExit(sequenceProcess);
            FullMigrationToolProcess primaryKeyProcess = new FullMigrationToolProcess(
                    ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_PRIMARY_KEY, taskWorkspace, fullConfig,
                    FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    FullMigrationToolConstants.ORDER_PRIMARY_KEY, jvmPrefix);
            startProcessAndWaitExit(primaryKeyProcess);
            FullMigrationToolProcess indexProcess = new FullMigrationToolProcess(
                    ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_INDEX, taskWorkspace, fullConfig,
                    FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_INDEX,
                    jvmPrefix);
            startProcessAndWaitExit(indexProcess);
            FullMigrationToolProcess constraintProcess = new FullMigrationToolProcess(
                    ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_CONSTRAINT, taskWorkspace, fullConfig,
                    FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    FullMigrationToolConstants.ORDER_CONSTRAINT, jvmPrefix);
            startProcessAndWaitExit(constraintProcess);
        }
    }

    /**
     * Start migration trigger and wait exit
     */
    protected void triggerMigration() {
        FullMigrationToolProcess triggerProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_TRIGGER, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_TRIGGER,
                jvmPrefix);
        startProcessAndWaitExit(triggerProcess);
    }

    /**
     * Start migration view and wait exit
     */
    protected void viewMigration() {
        FullMigrationToolProcess viewProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_VIEW, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_VIEW,
                jvmPrefix);
        startProcessAndWaitExit(viewProcess);
    }

    /**
     * Start migration function and wait exit
     */
    protected void functionMigration() {
        FullMigrationToolProcess functionProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_FUNCTION, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_FUNCTION,
                jvmPrefix);
        startProcessAndWaitExit(functionProcess);
    }

    /**
     * Start migration procedure and wait exit
     */
    protected void procedureMigration() {
        FullMigrationToolProcess procedureProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_PROCEDURE, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_PROCEDURE,
                jvmPrefix);
        startProcessAndWaitExit(procedureProcess);
    }

    /**
     * Start migration foreign key and wait exit
     */
    protected void foreignKeyMigration() {
        FullMigrationToolProcess foreignKeyProcess = new FullMigrationToolProcess(
                ProcessNameConstants.FULL_MIGRATION_TOOL_MIGRATION_FOREIGN_KEY, taskWorkspace, fullConfig,
                FullMigrationToolConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, FullMigrationToolConstants.ORDER_FOREIGN_KEY,
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
