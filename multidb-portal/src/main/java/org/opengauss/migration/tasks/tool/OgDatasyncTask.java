/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.tool.OgDatasyncConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.OgDatasyncConfigBundle;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.migration.process.task.OgDatasyncProcess;
import org.opengauss.migration.process.task.TaskProcess;

/**
 * oG_datasync_full_migration task
 *
 * @since 2025/5/29
 */
public abstract class OgDatasyncTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(OgDatasyncTask.class);

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

    protected OgDatasyncTask(TaskWorkspace taskWorkspace, MigrationStopIndicator migrationStopIndicator,
                             OgDatasyncConfigBundle fullMigrationToolConfig, String jvmPrefix) {
        super(taskWorkspace);
        this.migrationStopIndicator = migrationStopIndicator;
        this.fullConfig = fullMigrationToolConfig.getConfigFile();
        this.jvmPrefix = jvmPrefix;
    }

    /**
     * Drop replica schema
     */
    protected void dropReplicaSchema() {
        OgDatasyncProcess dropSchemaProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_DROP_REPLICA_SCHEMA, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                OgDatasyncConstants.ORDER_DROP_REPLICA_SCHEMA,
                jvmPrefix);
        dropSchemaProcess.start();
        dropSchemaProcess.waitExit();
    }

    /**
     * Start migration table
     */
    protected void tableMigration() {
        OgDatasyncProcess tableProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_TABLE, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_TABLE,
                jvmPrefix);

        currentProcess = tableProcess;
        tableProcess.start();
    }

    /**
     * Wait table migration exit
     */
    protected void waitTableMigrationExit() {
        if (currentProcess != null
                && currentProcess.getProcessName().equals(ProcessNameConstants.OG_DATASYNC_MIGRATION_TABLE)) {
            currentProcess.waitExit();
            if (!currentProcess.checkStatus()) {
                throw new MigrationException("Failed to migrate table, please check the log for details.");
            }

            OgDatasyncProcess sequenceProcess = new OgDatasyncProcess(
                    ProcessNameConstants.OG_DATASYNC_MIGRATION_SEQUENCE, taskWorkspace, fullConfig,
                    OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_SEQUENCE,
                    jvmPrefix);
            startProcessAndWaitExit(sequenceProcess);
            OgDatasyncProcess primaryKeyProcess = new OgDatasyncProcess(
                    ProcessNameConstants.OG_DATASYNC_MIGRATION_PRIMARY_KEY, taskWorkspace, fullConfig,
                    OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    OgDatasyncConstants.ORDER_PRIMARY_KEY, jvmPrefix);
            startProcessAndWaitExit(primaryKeyProcess);
            OgDatasyncProcess indexProcess = new OgDatasyncProcess(
                    ProcessNameConstants.OG_DATASYNC_MIGRATION_INDEX, taskWorkspace, fullConfig,
                    OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_INDEX,
                    jvmPrefix);
            startProcessAndWaitExit(indexProcess);
            OgDatasyncProcess constraintProcess = new OgDatasyncProcess(
                    ProcessNameConstants.OG_DATASYNC_MIGRATION_CONSTRAINT, taskWorkspace, fullConfig,
                    OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL,
                    OgDatasyncConstants.ORDER_CONSTRAINT, jvmPrefix);
            startProcessAndWaitExit(constraintProcess);
        }
    }

    /**
     * Start migration trigger and wait exit
     */
    protected void triggerMigration() {
        OgDatasyncProcess triggerProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_TRIGGER, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_TRIGGER,
                jvmPrefix);
        startProcessAndWaitExit(triggerProcess);
    }

    /**
     * Start migration view and wait exit
     */
    protected void viewMigration() {
        OgDatasyncProcess viewProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_VIEW, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_VIEW,
                jvmPrefix);
        startProcessAndWaitExit(viewProcess);
    }

    /**
     * Start migration function and wait exit
     */
    protected void functionMigration() {
        OgDatasyncProcess functionProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_FUNCTION, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_FUNCTION,
                jvmPrefix);
        startProcessAndWaitExit(functionProcess);
    }

    /**
     * Start migration procedure and wait exit
     */
    protected void procedureMigration() {
        OgDatasyncProcess procedureProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_PROCEDURE, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_PROCEDURE,
                jvmPrefix);
        startProcessAndWaitExit(procedureProcess);
    }

    /**
     * Start migration foreign key and wait exit
     */
    protected void foreignKeyMigration() {
        OgDatasyncProcess foreignKeyProcess = new OgDatasyncProcess(
                ProcessNameConstants.OG_DATASYNC_MIGRATION_FOREIGN_KEY, taskWorkspace, fullConfig,
                OgDatasyncConstants.SUPPORT_SOURCE_DB_TYPE_PGSQL, OgDatasyncConstants.ORDER_FOREIGN_KEY,
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
