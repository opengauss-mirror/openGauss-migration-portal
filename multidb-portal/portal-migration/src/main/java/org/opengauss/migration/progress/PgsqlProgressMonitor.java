/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress;

import com.alibaba.fastjson2.JSON;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.FullReplicateConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.migration.helper.tool.FullReplicateHelper;
import org.opengauss.migration.progress.model.FullEntry;
import org.opengauss.migration.progress.model.FullTotalInfo;
import org.opengauss.migration.progress.model.tool.FullReplicateStatusEntry;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * pgsql progress monitor
 *
 * @since 2025/4/1
 */
public class PgsqlProgressMonitor extends ProgressMonitor {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlProgressMonitor.class);

    PgsqlProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        super(statusMonitor, taskWorkspace);
    }

    @Override
    void readFullMigrationProgress() {
        String tableJsonPath = FullReplicateHelper.generateOrderStatusFilePath(taskWorkspace,
                FullReplicateConstants.ORDER_TABLE);
        if (isFileModified(tableJsonPath)) {
            readTableProgress(tableJsonPath);
        }

        String viewJsonPath = FullReplicateHelper.generateOrderStatusFilePath(taskWorkspace,
                FullReplicateConstants.ORDER_VIEW);
        if (isFileModified(viewJsonPath)) {
            readViewProgress(viewJsonPath);
        }

        String funcJsonPath = FullReplicateHelper.generateOrderStatusFilePath(taskWorkspace,
                FullReplicateConstants.ORDER_FUNCTION);
        if (isFileModified(funcJsonPath)) {
            readFuncProgress(funcJsonPath);
        }

        String triggerJsonPath = FullReplicateHelper.generateOrderStatusFilePath(taskWorkspace,
                FullReplicateConstants.ORDER_TRIGGER);
        if (isFileModified(triggerJsonPath)) {
            readTriggerProgress(triggerJsonPath);
        }

        String procJsonPath = FullReplicateHelper.generateOrderStatusFilePath(taskWorkspace,
                FullReplicateConstants.ORDER_PROCEDURE);
        if (isFileModified(procJsonPath)) {
            readProcProgress(procJsonPath);
        }
    }

    @Override
    void readFullDataCheckProgress() {

    }

    @Override
    void readIncrementalMigrationProgress() {
        super.readDebeziumIncrementalMigrationProgress();
    }

    @Override
    void readReverseMigrationProgress() {
        super.readDebeziumReverseMigrationProgress();
    }

    private void readTableProgress(String filePath) {
        Optional<FullReplicateStatusEntry> entryOptional = FullReplicateHelper.parseToolStatusFile(filePath);
        if (entryOptional.isEmpty()) {
            return;
        }

        FullReplicateStatusEntry statusEntry = entryOptional.get();
        FullTotalInfo total = statusEntry.getTotal();
        if (total != null) {
            String totalJsonString = JSON.toJSONString(total);
            String totalStatusFilePath = MigrationStatusHelper.generateFullTotalInfoStatusFilePath(taskWorkspace);

            try {
                FileUtils.writeToFile(totalStatusFilePath, totalJsonString, false);
            } catch (IOException e) {
                LOGGER.warn("Failed to write full migration total status, error: {}", e.getMessage());
            }
        }

        List<FullEntry> tableList = statusEntry.getTable();
        tableList = filterIntegrityEntry(tableList);
        writeObjectEntryList(tableList, MigrationStatusHelper.generateFullTableStatusFilePath(taskWorkspace));
    }

    private void readViewProgress(String jsonPath) {
        Optional<FullReplicateStatusEntry> entryOptional = FullReplicateHelper.parseToolStatusFile(jsonPath);
        if (entryOptional.isEmpty()) {
            return;
        }

        List<FullEntry> viewList = entryOptional.get().getView();
        viewList = filterIntegrityEntry(viewList);
        writeObjectEntryList(viewList, MigrationStatusHelper.generateFullViewStatusFilePath(taskWorkspace));
    }

    private void readFuncProgress(String jsonPath) {
        Optional<FullReplicateStatusEntry> entryOptional = FullReplicateHelper.parseToolStatusFile(jsonPath);
        if (entryOptional.isEmpty()) {
            return;
        }

        List<FullEntry> funcList = entryOptional.get().getFunction();
        funcList = filterIntegrityEntry(funcList);
        writeObjectEntryList(funcList, MigrationStatusHelper.generateFullFuncStatusFilePath(taskWorkspace));
    }

    private void readTriggerProgress(String jsonPath) {
        Optional<FullReplicateStatusEntry> entryOptional = FullReplicateHelper.parseToolStatusFile(jsonPath);
        if (entryOptional.isEmpty()) {
            return;
        }

        List<FullEntry> triggerList = entryOptional.get().getTrigger();
        triggerList = filterIntegrityEntry(triggerList);
        writeObjectEntryList(triggerList, MigrationStatusHelper.generateFullTriggerStatusFilePath(taskWorkspace));
    }

    private void readProcProgress(String jsonPath) {
        Optional<FullReplicateStatusEntry> entryOptional = FullReplicateHelper.parseToolStatusFile(jsonPath);
        if (entryOptional.isEmpty()) {
            return;
        }

        List<FullEntry> procList = entryOptional.get().getProcedure();
        procList = filterIntegrityEntry(procList);
        writeObjectEntryList(procList, MigrationStatusHelper.generateFullProcStatusFilePath(taskWorkspace));
    }

    private List<FullEntry> filterIntegrityEntry(List<FullEntry> entryList) {
        if (entryList == null || entryList.isEmpty()) {
            return entryList;
        }

        boolean isIntegrity = true;
        List<FullEntry> integrityEntryList = new ArrayList<>();
        for (FullEntry entry : entryList) {
            if (entry.getStatus() == 0
                    || StringUtils.isNullOrBlank(entry.getName())
                    || StringUtils.isNullOrBlank(entry.getSchema())) {
                isIntegrity = false;
                continue;
            }
            integrityEntryList.add(entry);
        }

        if (!isIntegrity) {
            LOGGER.warn("PostgreSQL full migration progress file contains not integrity progress entry");
        }
        return integrityEntryList;
    }
}
