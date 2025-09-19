/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.migration.helper.tool.DebeziumHelper;
import org.opengauss.migration.handler.ThreadExceptionHandler;
import org.opengauss.migration.MigrationManager;
import org.opengauss.migration.process.task.DebeziumProcess;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.ThreadUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * process monitor
 *
 * @since 2025/3/1
 */
public class ProcessMonitor extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(ProcessMonitor.class);
    private static final int INTERVAL_TIME = 500;
    private static final int MAX_NOT_MODIFIED_COUNT = 60;
    private static final HashMap<String, Long> fileLastModifiedCache = new HashMap<>();
    private static final HashMap<String, Integer> fileNotModifiedCountCache = new HashMap<>();

    private final List<TaskProcess> taskProcessList = new CopyOnWriteArrayList<>();
    private final List<ConfluentProcess> confluentProcessList = new ArrayList<>();

    private volatile boolean isRunning = true;
    private StatusMonitor statusMonitor;
    private ProcessErrorHandler processErrorHandler;

    public ProcessMonitor() {
        super("Process-Monitor-Thread");
    }

    /**
     * Start monitoring
     *
     * @param migrationManager migration manager
     * @param statusMonitor status manager
     */
    public void startMonitoring(MigrationManager migrationManager, StatusMonitor statusMonitor) {
        this.statusMonitor = statusMonitor;
        this.processErrorHandler = new ProcessErrorHandler(migrationManager, statusMonitor);
        setDaemon(true);
        start();
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        confluentProcessList.addAll(Kafka.getInstance().getConfluentProcessList());
        while (isRunning) {
            ThreadUtils.sleep(500);

            for (TaskProcess taskProcess : taskProcessList) {
                if (!taskProcess.checkStatus()) {
                    taskProcessList.remove(taskProcess);
                    processErrorHandler.handleTaskProcessError(taskProcess);
                    break;
                }

                if (taskProcess.isStopped()) {
                    taskProcessList.remove(taskProcess);
                }

                if (!isProcessFunctional(taskProcess)) {
                    taskProcessList.remove(taskProcess);
                    taskProcess.stop();
                    processErrorHandler.handleTaskProcessError(taskProcess);
                    break;
                }
            }

            if (statusMonitor.isFullMigrationStatus() || statusMonitor.isNotRunning()) {
                continue;
            }

            for (ConfluentProcess confluentProcess : confluentProcessList) {
                if (!confluentProcess.checkStatus()) {
                    processErrorHandler.handleConfluentError();
                    break;
                }
            }
        }
        LOGGER.info("Process monitor has stopped.");
    }

    /**
     * Stop monitoring
     */
    public void stopMonitoring() {
        this.isRunning = false;
    }

    /**
     * Add process
     *
     * @param process task process
     */
    public void addProcess(TaskProcess process) {
        taskProcessList.add(process);
    }

    private boolean isProcessFunctional(TaskProcess process) {
        if (!(process instanceof DebeziumProcess)) {
            return true;
        }

        String processName = process.getProcessName();
        if (ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SOURCE.equals(processName)) {
            String statusFilePath = DebeziumHelper.getIncrementalSourceStatusFilePath(process.getTaskWorkspace());
            return isProcessStatusFileFunctional(processName, statusFilePath);
        }

        if (ProcessNameConstants.DEBEZIUM_INCREMENTAL_CONNECT_SINK.equals(processName)) {
            String statusFilePath = DebeziumHelper.getIncrementalSinkStatusFilePath(process.getTaskWorkspace());
            return isProcessStatusFileFunctional(processName, statusFilePath);
        }

        if (ProcessNameConstants.DEBEZIUM_REVERSE_CONNECT_SOURCE.equals(processName)) {
            String statusFilePath = DebeziumHelper.getReverseSourceStatusFilePath(process.getTaskWorkspace());
            return isProcessStatusFileFunctional(processName, statusFilePath);
        }

        if (ProcessNameConstants.DEBEZIUM_REVERSE_CONNECT_SINK.equals(processName)) {
            String statusFilePath = DebeziumHelper.getReverseSinkStatusFilePath(process.getTaskWorkspace());
            return isProcessStatusFileFunctional(processName, statusFilePath);
        }
        return true;
    }

    private boolean isProcessStatusFileFunctional(String processName, String statusFilePath) {
        if (isFileModified(statusFilePath)) {
            fileNotModifiedCountCache.put(statusFilePath, 0);
        } else {
            Integer cacheCount = fileNotModifiedCountCache.getOrDefault(statusFilePath, 0);
            if (cacheCount >= MAX_NOT_MODIFIED_COUNT) {
                LOGGER.error("Process '{}' status file is not modified for {} millis", processName,
                        INTERVAL_TIME * MAX_NOT_MODIFIED_COUNT);
                fileNotModifiedCountCache.put(statusFilePath, 0);
                return false;
            }
            fileNotModifiedCountCache.put(statusFilePath, cacheCount + 1);
        }
        return true;
    }

    private boolean isFileModified(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return true;
        }

        long lastModified = file.lastModified();
        Long cacheModified = fileLastModifiedCache.get(filePath);
        if (cacheModified == null || lastModified != cacheModified) {
            fileLastModifiedCache.put(filePath, lastModified);
            return true;
        }
        return false;
    }
}
