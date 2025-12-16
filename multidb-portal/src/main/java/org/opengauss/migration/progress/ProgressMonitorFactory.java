/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress;

import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.migration.status.StatusMonitor;

/**
 * progress monitor factory
 *
 * @since 2025/4/1
 */
public class ProgressMonitorFactory {
    private ProgressMonitorFactory() {
    }

    /**
     * create progress monitor
     *
     * @param sourceDbType source database type
     * @param statusMonitor status manager
     * @param taskWorkspace task workspace
     * @return progress monitor
     */
    public static ProgressMonitor createProgressMonitor(
            DatabaseType sourceDbType, StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        if (sourceDbType.equals(DatabaseType.MYSQL)) {
            return new MysqlProgressMonitor(statusMonitor, taskWorkspace);
        }
        if (sourceDbType.equals(DatabaseType.POSTGRESQL)) {
            return new PgsqlProgressMonitor(statusMonitor, taskWorkspace);
        }
        if (sourceDbType.equals(DatabaseType.MILVUS)) {
            return new MilvusProgressMonitor(statusMonitor, taskWorkspace);
        }
        if (sourceDbType.equals(DatabaseType.ELASTICSEARCH)) {
            return new ElasticsearchProgressMonitor(statusMonitor, taskWorkspace);
        }
        throw new ConfigException("Unsupported database type");
    }
}
