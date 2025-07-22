/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.ChameleonConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.migration.helper.tool.ChameleonHelper;
import org.opengauss.migration.helper.tool.DataCheckerHelper;
import org.opengauss.migration.progress.model.CheckEntry;
import org.opengauss.migration.progress.model.CheckFailEntry;
import org.opengauss.migration.progress.model.FullEntry;
import org.opengauss.migration.progress.model.FullTotalInfo;
import org.opengauss.migration.progress.model.tool.ChameleonStatusEntry;
import org.opengauss.migration.status.StatusMonitor;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * mysql progress monitor
 *
 * @since 2025/4/1
 */
public class MysqlProgressMonitor extends ProgressMonitor {
    private static final Logger LOGGER = LogManager.getLogger(MysqlProgressMonitor.class);

    MysqlProgressMonitor(StatusMonitor statusMonitor, TaskWorkspace taskWorkspace) {
        super(statusMonitor, taskWorkspace);
    }

    @Override
    void readFullMigrationProgress() {
        String tableJsonPath = ChameleonHelper.generateOrderStatusFilePath(taskWorkspace,
                ChameleonConstants.ORDER_INIT_REPLICA);
        if (isFileModified(tableJsonPath)) {
            readTableProgress(tableJsonPath);
        }

        String viewJsonPath = ChameleonHelper.generateOrderStatusFilePath(taskWorkspace,
                ChameleonConstants.ORDER_START_VIEW_REPLICA);
        if (isFileModified(viewJsonPath)) {
            readViewProgress(viewJsonPath);
        }

        String funcJsonPath = ChameleonHelper.generateOrderStatusFilePath(taskWorkspace,
                ChameleonConstants.ORDER_START_FUNC_REPLICA);
        if (isFileModified(funcJsonPath)) {
            readFuncProgress(funcJsonPath);
        }

        String triggerJsonPath = ChameleonHelper.generateOrderStatusFilePath(taskWorkspace,
                ChameleonConstants.ORDER_START_TRIGGER_REPLICA);
        if (isFileModified(triggerJsonPath)) {
            readTriggerProgress(triggerJsonPath);
        }

        String procJsonPath = ChameleonHelper.generateOrderStatusFilePath(taskWorkspace,
                ChameleonConstants.ORDER_START_PROC_REPLICA);
        if (isFileModified(procJsonPath)) {
            readProcProgress(procJsonPath);
        }
    }

    @Override
    void readFullDataCheckProgress() {
        String checkResultSuccessFilePath = DataCheckerHelper.getFullCheckResultSuccessFilePath(taskWorkspace);
        if (isFileModified(checkResultSuccessFilePath)) {
            readFullCheckSuccessProgress(checkResultSuccessFilePath);
        }

        String checkResultFailedFilePath = DataCheckerHelper.getFullCheckResultFailedFilePath(taskWorkspace);
        if (isFileModified(checkResultFailedFilePath)) {
            readFullCheckFailedProgress(checkResultFailedFilePath);
        }
    }

    @Override
    void readIncrementalMigrationProgress() {
        super.readDebeziumIncrementalMigrationProgress();
    }

    @Override
    void readReverseMigrationProgress() {
        super.readDebeziumReverseMigrationProgress();
    }

    private void readFullCheckSuccessProgress(String filePath) {
        Optional<JSONArray> successArrayOptional = DataCheckerHelper.parseDataCheckerStatusFile(filePath);
        if (successArrayOptional.isEmpty()) {
            return;
        }

        List<CheckEntry> checkEntryList = new ArrayList<>();
        JSONArray successArray = successArrayOptional.get();
        for (int i = 0; i < successArray.size(); i++) {
            JSONObject jsonObj = successArray.getJSONObject(i);
            CheckEntry checkEntry = new CheckEntry();
            checkEntry.setSchema(jsonObj.getString("schema"));
            checkEntry.setName(jsonObj.getString("table"));
            checkEntryList.add(checkEntry);
        }

        try {
            String statusPath = MigrationStatusHelper.generateFullCheckSuccessObjectStatusFilePath(taskWorkspace);
            FileUtils.writeToFile(statusPath, JSON.toJSONString(checkEntryList), false);
        } catch (IOException e) {
            LOGGER.warn("Failed to write full data check success status, error: {}", e.getMessage());
        }
    }

    private void readFullCheckFailedProgress(String filePath) {
        Optional<JSONArray> failedArrayOptional = DataCheckerHelper.parseDataCheckerStatusFile(filePath);
        if (failedArrayOptional.isEmpty()) {
            return;
        }

        List<CheckFailEntry> checkFailEntryList = new ArrayList<>();
        JSONArray failedArray = failedArrayOptional.get();
        for (int i = 0; i < failedArray.size(); i++) {
            JSONObject jsonObj = failedArray.getJSONObject(i);
            CheckFailEntry checkFailEntry = new CheckFailEntry();
            String schema = jsonObj.getString("schema");
            String table = jsonObj.getString("table");
            String repairPath = DataCheckerHelper.generateFullCheckResultRepairFilePath(taskWorkspace, schema, table);

            checkFailEntry.setSchema(schema);
            checkFailEntry.setName(table);
            checkFailEntry.setError(jsonObj.getString("message"));
            checkFailEntry.setRepairFilePath(repairPath);
            checkFailEntryList.add(checkFailEntry);
        }

        try {
            String failedStatusPath = MigrationStatusHelper.generateFullCheckFailedObjectStatusFilePath(taskWorkspace);
            FileUtils.writeToFile(failedStatusPath, JSON.toJSONString(checkFailEntryList), false);
        } catch (IOException e) {
            LOGGER.warn("Failed to write full data check failed status, error: {}", e.getMessage());
        }
    }

    private void readTableProgress(String filePath) {
        Optional<ChameleonStatusEntry> statusEntryOptional = ChameleonHelper.parseChameleonStatusFile(filePath);
        if (statusEntryOptional.isEmpty()) {
            return;
        }

        ChameleonStatusEntry statusEntry = statusEntryOptional.get();
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
        if (isEntryIntegrity(tableList)) {
            writeObjectEntryList(tableList, MigrationStatusHelper.generateFullTableStatusFilePath(taskWorkspace));
        }
    }

    private void readTriggerProgress(String filePath) {
        Optional<ChameleonStatusEntry> statusEntryOptional = ChameleonHelper.parseChameleonStatusFile(filePath);
        if (statusEntryOptional.isEmpty()) {
            return;
        }
        List<FullEntry> entryList = statusEntryOptional.get().getTrigger();
        if (isEntryIntegrity(entryList)) {
            writeObjectEntryList(entryList, MigrationStatusHelper.generateFullTriggerStatusFilePath(taskWorkspace));
        }
    }

    private void readViewProgress(String filePath) {
        Optional<ChameleonStatusEntry> statusEntryOptional = ChameleonHelper.parseChameleonStatusFile(filePath);
        if (statusEntryOptional.isEmpty()) {
            return;
        }
        List<FullEntry> entryList = statusEntryOptional.get().getView();
        if (isEntryIntegrity(entryList)) {
            writeObjectEntryList(entryList, MigrationStatusHelper.generateFullViewStatusFilePath(taskWorkspace));
        }
    }

    private void readFuncProgress(String filePath) {
        Optional<ChameleonStatusEntry> statusEntryOptional = ChameleonHelper.parseChameleonStatusFile(filePath);
        if (statusEntryOptional.isEmpty()) {
            return;
        }
        List<FullEntry> entryList = statusEntryOptional.get().getFunction();
        if (isEntryIntegrity(entryList)) {
            writeObjectEntryList(entryList, MigrationStatusHelper.generateFullFuncStatusFilePath(taskWorkspace));
        }
    }

    private void readProcProgress(String filePath) {
        Optional<ChameleonStatusEntry> statusEntryOptional = ChameleonHelper.parseChameleonStatusFile(filePath);
        if (statusEntryOptional.isEmpty()) {
            return;
        }
        List<FullEntry> entryList = statusEntryOptional.get().getProcedure();
        if (isEntryIntegrity(entryList)) {
            writeObjectEntryList(entryList, MigrationStatusHelper.generateFullProcStatusFilePath(taskWorkspace));
        }
    }

    private boolean isEntryIntegrity(List<FullEntry> entryList) {
        if (entryList == null || entryList.isEmpty()) {
            return true;
        }

        for (FullEntry entry : entryList) {
            if (entry.getStatus() == 0) {
                return false;
            }

            if (StringUtils.isNullOrBlank(entry.getName())) {
                return false;
            }
        }
        return true;
    }
}
