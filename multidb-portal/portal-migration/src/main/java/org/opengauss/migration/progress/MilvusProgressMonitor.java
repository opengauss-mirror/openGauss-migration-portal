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
 * MilvusProgressMonitor
 *
 * @since 2025/10/23
 */
public class MilvusProgressMonitor extends ProgressMonitor {
    private static final Logger LOGGER = LogManager.getLogger(MilvusProgressMonitor.class);

    private final Queue<String> completedCollectionQueue = new ConcurrentLinkedQueue<>();
    private final Queue<String> failedCollectionQueue = new ConcurrentLinkedQueue<>();

    MilvusProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        super(statusMonitor, taskWorkspace);
    }

    /**
     * Add migration completed collection name
     *
     * @param collectionName collection name
     */
    public void addCompletedCollection(String collectionName) {
        completedCollectionQueue.offer(collectionName);
    }

    /**
     * Add migration failed collection name
     *
     * @param collectionName collection name
     */
    public void addFailedCollection(String collectionName) {
        failedCollectionQueue.offer(collectionName);
    }

    @Override
    void readFullMigrationProgress() {
        String successFilePath = MigrationStatusHelper.generateFullSuccessTableStatusFilePath(taskWorkspace);
        String failedFilePath = MigrationStatusHelper.generateFullFailedTableStatusFilePath(taskWorkspace);
        saveQueuedProgress(completedCollectionQueue, successFilePath);
        saveQueuedProgress(failedCollectionQueue, failedFilePath);
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

        StringBuilder collections = new StringBuilder();
        List<String> tempList = new ArrayList<>();

        int count = queue.size();
        for (int i = 0; i < count; i++) {
            String collection = queue.poll();
            if (collection != null) {
                tempList.add(collection);
                collections.append(collection).append("\n");
            }
        }

        try {
            FileUtils.writeToFile(filePath, collections.toString(), true);
        } catch (IOException e) {
            LOGGER.warn("Failed to write Milvus full migration progress, error: {}", e.getMessage());
            queue.addAll(tempList);
        }
    }
}
