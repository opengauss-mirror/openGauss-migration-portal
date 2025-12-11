/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * ElasticsearchProgressMonitor
 *
 * @since 2025/10/23
 */
public class ElasticsearchProgressMonitor extends ProgressMonitor {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchProgressMonitor.class);

    private final Queue<String> completedIndexQueue = new ConcurrentLinkedQueue<>();
    private final Queue<String> failedIndexQueue = new ConcurrentLinkedQueue<>();

    ElasticsearchProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        super(statusMonitor, taskWorkspace);
    }

    /**
     * Add migration completed collection name
     *
     * @param indexName collection name
     */
    public void addCompletedIndex(String indexName) {
        completedIndexQueue.offer(indexName);
    }

    /**
     * Add migration failed index name
     *
     * @param indexName index name
     */
    public void addFailedIndex(String indexName) {
        failedIndexQueue.offer(indexName);
    }

    @Override
    void readFullMigrationProgress() {
        String successFilePath = MigrationStatusHelper.generateFullSuccessTableStatusFilePath(taskWorkspace);
        String failedFilePath = MigrationStatusHelper.generateFullFailedTableStatusFilePath(taskWorkspace);
        saveQueuedProgress(completedIndexQueue, successFilePath);
        saveQueuedProgress(failedIndexQueue, failedFilePath);
    }

    @Override
    void readFullDataCheckProgress() {
    }

    @Override
    void readIncrementalMigrationProgress() {
    }

    @Override
    void readReverseMigrationProgress() {
    }

    private void saveQueuedProgress(Queue<String> queue, String filePath) {
        if (queue.isEmpty()) {
            return;
        }

        StringBuilder indexes = new StringBuilder();
        List<String> tempList = new ArrayList<>();

        int count = queue.size();
        for (int i = 0; i < count; i++) {
            String index = queue.poll();
            if (index != null) {
                tempList.add(index);
                indexes.append(index).append("\n");
            }
        }

        try {
            FileUtils.writeToFile(filePath, indexes.toString(), true);
        } catch (IOException e) {
            LOGGER.warn("Failed to write Elasticsearch full migration progress, error: {}", e.getMessage());
            queue.addAll(tempList);
        }
    }
}
