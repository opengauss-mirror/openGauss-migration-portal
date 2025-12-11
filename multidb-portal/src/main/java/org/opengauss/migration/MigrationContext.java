/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration;

import lombok.Getter;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.config.AbstractMigrationJobConfig;
import org.opengauss.migration.config.ElasticsearchMigrationJobConfig;
import org.opengauss.migration.config.MilvusMigrationJobConfig;
import org.opengauss.migration.config.MysqlMigrationJobConfig;
import org.opengauss.migration.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.job.AbstractMigrationJob;
import org.opengauss.migration.job.ElasticsearchMigrationJob;
import org.opengauss.migration.job.MilvusMigrationJob;
import org.opengauss.migration.job.MysqlMigrationJob;
import org.opengauss.migration.job.PgsqlMigrationJob;
import org.opengauss.migration.monitor.MigrationAliveMonitor;
import org.opengauss.migration.process.ProcessMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitorFactory;
import org.opengauss.migration.status.StatusMonitor;

/**
 * MigrationContext
 *
 * @since 2025/9/15
 */
@Getter
public class MigrationContext {
    private static volatile MigrationContext instance;

    private TaskWorkspace taskWorkspace;
    private DatabaseType sourceDbType;

    private MigrationStopIndicator migrationStopIndicator;

    private ProgressMonitor progressMonitor;
    private ProcessMonitor processMonitor;
    private StatusMonitor statusMonitor;
    private MigrationAliveMonitor migrationAliveMonitor;

    private AbstractMigrationJobConfig migrationJobConfig;
    private AbstractMigrationJob migrationJob;

    private MigrationContext() {
    }

    /**
     * Get migration context
     *
     * @return MigrationContext migration context
     */
    public static MigrationContext getInstance() {
        if (instance == null) {
            synchronized (MigrationContext.class) {
                if (instance == null) {
                    throw new IllegalStateException("Migration context has not initialized");
                }
            }
        }
        return instance;
    }

    /**
     * Initialize migration context
     *
     * @param taskWorkspace task workspace
     */
    public static void initialize(TaskWorkspace taskWorkspace) {
        if (instance == null) {
            synchronized (MigrationContext.class) {
                if (instance == null) {
                    initMigrationContext(taskWorkspace);
                }
            }
        } else {
            throw new IllegalStateException("Migration context already initialized");
        }
    }

    private static void initMigrationContext(TaskWorkspace taskWorkspace) {
        instance = new MigrationContext();
        instance.taskWorkspace = taskWorkspace;
        DatabaseType sourceDbType = TaskHelper.loadSourceDbType(taskWorkspace);
        instance.sourceDbType = sourceDbType;

        StatusMonitor statusMonitor = new StatusMonitor(taskWorkspace);
        instance.statusMonitor = statusMonitor;
        instance.progressMonitor = ProgressMonitorFactory.createProgressMonitor(
                sourceDbType, statusMonitor, taskWorkspace);
        instance.migrationStopIndicator = new MigrationStopIndicator();
        instance.migrationAliveMonitor = new MigrationAliveMonitor(taskWorkspace);
        instance.processMonitor = new ProcessMonitor();

        if (DatabaseType.MYSQL.equals(sourceDbType)) {
            MysqlMigrationJobConfig migrationJobConfig = new MysqlMigrationJobConfig(taskWorkspace);
            migrationJobConfig.loadConfig();
            instance.migrationJobConfig = migrationJobConfig;
            instance.migrationJob = new MysqlMigrationJob(migrationJobConfig);
        } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
            PgsqlMigrationJobConfig migrationJobConfig = new PgsqlMigrationJobConfig(taskWorkspace);
            migrationJobConfig.loadConfig();
            instance.migrationJobConfig = migrationJobConfig;
            instance.migrationJob = new PgsqlMigrationJob(migrationJobConfig);
        } else if (DatabaseType.MILVUS.equals(sourceDbType)) {
            MilvusMigrationJobConfig migrationJobConfig = new MilvusMigrationJobConfig(taskWorkspace);
            migrationJobConfig.loadConfig();
            instance.migrationJobConfig = migrationJobConfig;
            instance.migrationJob = new MilvusMigrationJob(migrationJobConfig);
        } else if (DatabaseType.ELASTICSEARCH.equals(sourceDbType)) {
            ElasticsearchMigrationJobConfig migrationJobConfig = new ElasticsearchMigrationJobConfig(taskWorkspace);
            migrationJobConfig.loadConfig();
            instance.migrationJobConfig = migrationJobConfig;
            instance.migrationJob = new ElasticsearchMigrationJob(migrationJobConfig);
        } else {
            throw new MigrationException("Unsupported source database type: " + sourceDbType);
        }
    }
}
