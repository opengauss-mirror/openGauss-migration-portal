/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tasks.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.ProcessNameConstants;
import org.opengauss.constants.config.MilvusMigrationToolConfig;
import org.opengauss.constants.tool.MilvusMigrationToolConstants;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.MigrationStopIndicator;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.migration.MigrationContext;
import org.opengauss.migration.helper.tool.MilvusMigrationToolHelper;
import org.opengauss.migration.process.task.MilvusMigrationToolProcess;
import org.opengauss.migration.process.task.TaskProcess;
import org.opengauss.migration.progress.MilvusProgressMonitor;
import org.opengauss.migration.progress.ProgressMonitor;
import org.opengauss.migration.tasks.ToolTask;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MilvusMigrationToolTask
 *
 * @since 2025/10/24
 */
public abstract class MilvusMigrationToolTask extends ToolTask {
    private static final Logger LOGGER = LogManager.getLogger(MilvusMigrationToolTask.class);

    private final ConfigFile configFile;
    private final Map<String, String> tableMappings;
    private final int migrationConcurrentThreads;
    private final ExecutorService executorService;
    private final BlockingQueue<Map.Entry<String, String>> workQueue;
    private final AtomicInteger migratedCount = new AtomicInteger(0);
    private final Set<TaskProcess> runningProcesses = ConcurrentHashMap.newKeySet();
    private final Map<String, String> processEnv;

    private MigrationStopIndicator migrationStopIndicator;
    private MilvusProgressMonitor milvusProgressMonitor;

    private volatile boolean hasProductionCompleted = false;

    protected MilvusMigrationToolTask(
            TaskWorkspace taskWorkspace, MilvusMigrationConfigDto migrationConfigDto, ConfigFile configFile,
            Map<String, String> tableMappings, int migrationConcurrentThreads
    ) {
        super(taskWorkspace);
        this.configFile = configFile;
        this.tableMappings = tableMappings;

        this.workQueue = new LinkedBlockingQueue<>(Math.min(1024, tableMappings.size()));
        this.migrationConcurrentThreads = Math.min(Math.max(1, migrationConcurrentThreads), tableMappings.size());
        int maxPoolSizes = migrationConcurrentThreads + 1;
        this.executorService = new ThreadPoolExecutor(
                maxPoolSizes,
                maxPoolSizes,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
        this.processEnv = generateProcessEnv(migrationConfigDto);

        getMigrationContextInfo();
    }

    /**
     * Prepare migration
     */
    protected void prepareMigration() {
        cleanHistoryFiles();
    }

    /**
     * Start migration
     */
    protected void startMigration() {
        for (int i = 0; i < migrationConcurrentThreads; i++) {
            executorService.execute(this::consumer);
        }

        executorService.execute(this::producer);

        awaitMigrationCompletion();
        shutdownExecutorService();
    }

    /**
     * Stop task migration
     */
    protected void stopMigration() {
        runningProcesses.forEach(TaskProcess::stop);
        executorService.shutdownNow();
    }

    /**
     * After migration
     */
    protected void afterMigration() {
        shutdownExecutorService();
        cleanTmpFiles();
    }

    private void producer() {
        try {
            for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
                if (migrationStopIndicator != null && migrationStopIndicator.isStopped()) {
                    LOGGER.info("Migration stopped during production, skip remaining collection migration");
                    return;
                }

                workQueue.put(entry);
            }

            hasProductionCompleted = true;
            LOGGER.debug("Milvus migration task producer completed, total produced: {}", tableMappings.size());
        } catch (InterruptedException e) {
            LOGGER.warn("Milvus migration task producer thread interrupted");
            Thread.currentThread().interrupt();
        } finally {
            LOGGER.debug("Milvus migration task producer thread finished");
        }
    }

    private void consumer() {
        String threadName = Thread.currentThread().getName();
        LOGGER.debug("Milvus migration task consumer thread {} started", threadName);

        try {
            while (!migrationStopIndicator.isStopped() && (!hasProductionCompleted || !workQueue.isEmpty())) {
                Map.Entry<String, String> entry = workQueue.poll(1, TimeUnit.SECONDS);
                if (entry != null) {
                    migrationCollection(entry);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Milvus migration task consumer thread {} interrupted", threadName);
            Thread.currentThread().interrupt();
        } finally {
            LOGGER.debug("Milvus migration task consumer thread {} finished", threadName);
        }
    }

    private void migrationCollection(Map.Entry<String, String> entry) {
        try {
            if (migrationStopIndicator.isStopped()) {
                LOGGER.info("Migration task is stopped, skip remaining collection migration");
                return;
            }

            String collection = entry.getKey();
            TaskProcess process = new MilvusMigrationToolProcess(ProcessNameConstants.MILVUS_MIGRATION_TOOL,
                    taskWorkspace, collection, entry.getValue(), processEnv);
            runningProcesses.add(process);
            process.start();
            process.waitExit();
            runningProcesses.remove(process);

            if (process.checkStatus() && determineMigrationResult(collection)) {
                milvusProgressMonitor.addCompletedCollection(collection);
            } else {
                milvusProgressMonitor.addFailedCollection(collection);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to migrate Milvus collection '{}'", entry.getKey(), e);
            milvusProgressMonitor.addFailedCollection(entry.getKey());
        } finally {
            migratedCount.incrementAndGet();
        }
    }

    private boolean determineMigrationResult(String collection) {
        String logPath = MilvusMigrationToolHelper.generateFullMigrationLogPath(taskWorkspace, collection);
        try {
            String logContent = FileUtils.readFileContents(logPath);
            if (logContent.contains(MilvusMigrationToolConstants.MIGRATION_SUCCESS_FLAG)
                    && !logContent.contains(MilvusMigrationToolConstants.MIGRATION_FAILED_FLAG)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("Failed to read Milvus collection '{}' migration log file '{}'", collection, logPath, e);
        }
        return false;
    }

    private void awaitMigrationCompletion() {
        long checkInterval = 1000L;
        long logInterval = 30000L;
        long lastLogTime = System.currentTimeMillis();
        int totalCollections = tableMappings.size();

        while (migratedCount.get() < totalCollections && !migrationStopIndicator.isStopped()) {
            try {
                Thread.sleep(checkInterval);

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime >= logInterval) {
                    int migratedCollections = migratedCount.get();
                    LOGGER.info("Migration progress: {}/{} ({}%)", migratedCollections, totalCollections,
                            (migratedCollections * 100) / totalCollections);
                    lastLogTime = currentTime;
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Milvus migration task await thread has been interrupted");
                break;
            }
        }

        int finalCompleted = migratedCount.get();
        LOGGER.info("Migration finished: {}/{} collections", finalCompleted, totalCollections);
    }

    private void shutdownExecutorService() {
        if (executorService.isShutdown()) {
            return;
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                LOGGER.warn("Forcing migration executor service shutdown after timeout");
                executorService.shutdownNow();

                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    LOGGER.error("Migration executor service did not terminate properly");
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Migration executor service shutdown interrupted");
            executorService.shutdownNow();
        }
    }

    private void cleanTmpFiles() {
        String tmpDirPath = taskWorkspace.getTmpDirPath();
        try {
            FileUtils.cleanDirectory(tmpDirPath);
        } catch (IOException e) {
            LOGGER.warn("Clean tmp files failed, error: {}", e.getMessage());
        }
    }

    private void cleanHistoryFiles() {
        String tmpDirPath = taskWorkspace.getTmpDirPath();
        String logDirPath = taskWorkspace.getLogsFullDirPath();
        String statusDirPath = taskWorkspace.getStatusFullDirPath();
        try {
            FileUtils.cleanDirectory(tmpDirPath);
            FileUtils.cleanDirectory(logDirPath);
            FileUtils.cleanDirectory(statusDirPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to delete Milvus migration history files, error message: {}", e.getMessage());
        }
    }

    private Map<String, String> generateProcessEnv(MilvusMigrationConfigDto migrationConfigDto) {
        Map<String, String> env = new HashMap<>();
        env.put(MilvusMigrationToolConfig.ENABLE_ENV_PASSWORD, "true");
        env.put(MilvusMigrationToolConfig.OPENGAUSS_PASSWORD, migrationConfigDto.getOpengaussDatabasePassword());

        if (migrationConfigDto.isMilvusAnonymousAccess()) {
            return env;
        }

        env.put(MilvusMigrationToolConfig.MILVUS_PASSWORD, migrationConfigDto.getMilvusPassword());
        return env;
    }

    private void getMigrationContextInfo() {
        this.migrationStopIndicator = MigrationContext.getInstance().getMigrationStopIndicator();
        ProgressMonitor progressMonitor = MigrationContext.getInstance().getProgressMonitor();
        if (progressMonitor instanceof MilvusProgressMonitor) {
            this.milvusProgressMonitor = (MilvusProgressMonitor) progressMonitor;
        } else {
            throw new MigrationException("Progress monitor is not MilvusProgressMonitor, Milvus migration tool task "
                    + "can not report progress.");
        }
    }
}
