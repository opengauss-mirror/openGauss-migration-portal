/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.TaskConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.handler.PortalExceptionHandler;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Migration alive monitor
 *
 * @since 2025/7/2
 */
public class MigrationAliveMonitor {
    private static final Logger LOGGER = LogManager.getLogger(MigrationAliveMonitor.class);
    private static final long HEARTBEAT_INTERVAL = 1L;

    private ScheduledExecutorService executor;
    private TaskWorkspace workspace;

    public MigrationAliveMonitor(TaskWorkspace workspace) {
        this.workspace = workspace;
    }

    /**
     * Start heartbeat service
     */
    public void start() {
        if (executor != null && !executor.isShutdown()) {
            return;
        }

        String heartbeatFilePath = getHeartbeatFilePath(workspace);
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            Thread.currentThread().setUncaughtExceptionHandler(new PortalExceptionHandler());
            try {
                updateHeartbeat(heartbeatFilePath);
            } catch (IOException e) {
                LOGGER.warn("Failed to update heartbeat, error message:{}", e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * Stop heartbeat service
     */
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
            cleanup();
            executor = null;
            workspace = null;
        }
    }

    /**
     * Get heartbeat file path
     *
     * @param workspace task workspace
     * @return heartbeat file path
     */
    public static String getHeartbeatFilePath(TaskWorkspace workspace) {
        return String.format("%s/%s", workspace.getStatusDirPath(), TaskConstants.HEARTBEAT_FILE);
    }

    private void updateHeartbeat(String filePath) throws IOException {
        File heartbeatFile = new File(filePath);
        if (!heartbeatFile.exists()) {
            heartbeatFile.createNewFile();
        } else {
            heartbeatFile.setLastModified(System.currentTimeMillis());
        }
    }

    private void cleanup() {
        File heartbeatFile = new File(getHeartbeatFilePath(workspace));
        if (heartbeatFile.exists()) {
            heartbeatFile.delete();
        }
    }
}
