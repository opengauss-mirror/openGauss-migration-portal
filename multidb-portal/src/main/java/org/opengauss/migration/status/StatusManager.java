/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.status;

import com.alibaba.fastjson2.JSON;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.enums.MigrationStatusEnum;
import org.opengauss.exceptions.PortalException;
import org.opengauss.exceptions.TaskException;
import org.opengauss.migration.helper.MigrationStatusHelper;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.progress.model.CheckEntry;
import org.opengauss.migration.progress.model.CheckFailEntry;
import org.opengauss.migration.progress.model.FullEntry;
import org.opengauss.migration.progress.model.FullTotalInfo;
import org.opengauss.migration.progress.model.IncrementalAndReverseEntry;
import org.opengauss.migration.status.model.MilvusElasticsearchStatusEntry;
import org.opengauss.migration.status.model.ObjectStatusEntry;
import org.opengauss.utils.FileUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Migration status manager
 *
 * @since 2025/5/12
 */
public class StatusManager {
    private static final Logger LOGGER = LogManager.getLogger(StatusManager.class);

    private TaskWorkspace taskWorkspace;

    public StatusManager(TaskWorkspace taskWorkspace) {
        this.taskWorkspace = taskWorkspace;
    }

    /**
     * Get current migration status
     *
     * @return current migration status
     */
    public MigrationStatusEnum getCurrentMigrationStatus() {
        String statusFilePath = MigrationStatusHelper.generateMigrationStatusFilePath(taskWorkspace);
        try {
            String contents = FileUtils.readFileContents(statusFilePath);
            List<MigrationStatus> statusList = JSON.parseArray(contents, MigrationStatus.class);
            if (!statusList.isEmpty()) {
                return statusList.get(statusList.size() - 1).getStatus();
            } else {
                return MigrationStatusEnum.NOT_START;
            }
        } catch (FileNotFoundException e) {
            return MigrationStatusEnum.NOT_START;
        } catch (IOException e) {
            throw new PortalException("Failed to read migration status file: " + statusFilePath, e);
        }
    }

    /**
     * Get status
     *
     * @return status
     */
    public String getStatus() {
        MigrationStatusEnum currentStatus = getCurrentMigrationStatus();
        StringBuilder detailBuilder = new StringBuilder("=== Data Migration Progress ===");
        detailBuilder.append(System.lineSeparator()).append("Current migration status: ")
                .append(currentStatus.getDescription()).append(System.lineSeparator());

        List<MigrationStatusEnum> statusEnumList = readMigrationStatusList(taskWorkspace)
                .stream()
                .map(MigrationStatus::getStatus)
                .toList();

        if (statusEnumList.stream().anyMatch(MigrationStatusHelper::isFullMigrationStatus)) {
            appendFullMigrationStatus(detailBuilder);
        }

        if (statusEnumList.stream().anyMatch(MigrationStatusHelper::isFullDataCheckStatus)) {
            appendFullCheckStatus(detailBuilder);
        }

        if (statusEnumList.stream().anyMatch(MigrationStatusHelper::isIncrementalMigrationStatus)) {
            appendIncrementalMigrationStatus(detailBuilder);
        }

        if (statusEnumList.stream().anyMatch(MigrationStatusHelper::isReverseMigrationStatus)) {
            appendReverseMigrationStatus(detailBuilder);
        }
        return detailBuilder.toString();
    }

    /**
     * Get object status entry list for mysql source
     *
     * @return object status entry list
     */
    public List<ObjectStatusEntry> getMysqlObjectStatusEntryList() {
        List<MigrationStatusEnum> statusEnumList = readMigrationStatusList(taskWorkspace)
                .stream()
                .map(MigrationStatus::getStatus)
                .toList();
        if (statusEnumList.stream().noneMatch(MigrationStatusHelper::isFullMigrationStatus)) {
            return new ArrayList<>();
        }

        HashMap<String, ObjectStatusEntry> entryMap = new HashMap<>();
        List<FullEntry> fullTableList = getFullTableProgress();
        if (!fullTableList.isEmpty()) {
            for (FullEntry fullEntry : fullTableList) {
                ObjectStatusEntry objectStatusEntry = new ObjectStatusEntry(fullEntry, "table");
                entryMap.put(fullEntry.getName(), objectStatusEntry);
            }
        } else {
            return new ArrayList<>();
        }

        List<CheckEntry> checkProgress = getCheckProgress();
        if (checkProgress != null && !checkProgress.isEmpty()) {
            for (CheckEntry checkEntry : checkProgress) {
                ObjectStatusEntry objectStatusEntry = entryMap.get(checkEntry.getName());
                if (objectStatusEntry != null) {
                    objectStatusEntry.setCheckSuccessStatus();
                }
            }
        }

        List<CheckFailEntry> checkFailProgress = getCheckFailProgress();
        if (checkFailProgress != null && !checkFailProgress.isEmpty()) {
            for (CheckFailEntry checkFailEntry : checkFailProgress) {
                ObjectStatusEntry objectStatusEntry = entryMap.get(checkFailEntry.getName());
                if (objectStatusEntry != null) {
                    objectStatusEntry.setCheckFailStatus(checkFailEntry);
                }
            }
        }

        ArrayList<ObjectStatusEntry> result = entryMap.values().stream().sorted(ObjectStatusEntry::compareByName)
                .collect(Collectors.toCollection(ArrayList::new));
        getFullViewProgress().stream().sorted(FullEntry::compareByName)
                .forEach(entry -> result.add(new ObjectStatusEntry(entry, "view")));
        getFullFunctionProgress().stream().sorted(FullEntry::compareByName)
                .forEach(entry -> result.add(new ObjectStatusEntry(entry, "function")));
        getFullTriggerProgress().stream().sorted(FullEntry::compareByName)
                .forEach(entry -> result.add(new ObjectStatusEntry(entry, "trigger")));
        getFullProcedureProgress().stream().sorted(FullEntry::compareByName)
                .forEach(entry -> result.add(new ObjectStatusEntry(entry, "procedure")));
        return result;
    }

    /**
     * Get object status entry list for pgsql source
     *
     * @return object status entry list
     */
    public List<ObjectStatusEntry> getPgsqlObjectStatusEntryList() {
        List<MigrationStatusEnum> statusEnumList = readMigrationStatusList(taskWorkspace)
                .stream()
                .map(MigrationStatus::getStatus)
                .toList();

        if (statusEnumList.stream().noneMatch(MigrationStatusHelper::isFullMigrationStatus)) {
            return new ArrayList<>();
        }
        HashMap<String, ObjectStatusEntry> entryMap = new HashMap<>();
        List<FullEntry> fullTableList = getFullTableProgress();
        if (!fullTableList.isEmpty()) {
            for (FullEntry fullEntry : fullTableList) {
                String key = fullEntry.getSchema() + "." + fullEntry.getName();
                ObjectStatusEntry objectStatusEntry = new ObjectStatusEntry(fullEntry, "table");
                entryMap.put(key, objectStatusEntry);
            }
        } else {
            return new ArrayList<>();
        }

        List<CheckEntry> checkProgress = getCheckProgress();
        if (checkProgress != null && !checkProgress.isEmpty()) {
            for (CheckEntry checkEntry : checkProgress) {
                String key = checkEntry.getSchema() + "." + checkEntry.getName();
                ObjectStatusEntry objectStatusEntry = entryMap.get(key);
                if (objectStatusEntry != null) {
                    objectStatusEntry.setCheckSuccessStatus();
                }
            }
        }

        List<CheckFailEntry> checkFailProgress = getCheckFailProgress();
        if (checkFailProgress != null && !checkFailProgress.isEmpty()) {
            for (CheckFailEntry checkFailEntry : checkFailProgress) {
                String key = checkFailEntry.getSchema() + "." + checkFailEntry.getName();
                ObjectStatusEntry objectStatusEntry = entryMap.get(key);
                if (objectStatusEntry != null) {
                    objectStatusEntry.setCheckFailStatus(checkFailEntry);
                }
            }
        }

        ArrayList<ObjectStatusEntry> resultList = entryMap.values().stream().sorted(ObjectStatusEntry::compare)
                .collect(Collectors.toCollection(ArrayList::new));
        getFullViewProgress().stream().sorted(FullEntry::compare)
                .forEach(entry -> resultList.add(new ObjectStatusEntry(entry, "view")));
        getFullFunctionProgress().stream().sorted(FullEntry::compare)
                .forEach(entry -> resultList.add(new ObjectStatusEntry(entry, "function")));
        getFullTriggerProgress().stream().sorted(FullEntry::compare)
                .forEach(entry -> resultList.add(new ObjectStatusEntry(entry, "trigger")));
        getFullProcedureProgress().stream().sorted(FullEntry::compare)
                .forEach(entry -> resultList.add(new ObjectStatusEntry(entry, "procedure")));
        return resultList;
    }

    /**
     * Get Milvus/Elasticsearch status entry list
     *
     * @return Milvus/Elasticsearch status entry list
     */
    public List<MilvusElasticsearchStatusEntry> getMilvusElasticsearchStatusEntryList() {
        List<String> successTableList = getMilvusElasticsearchSuccessTableList();
        List<String> failedTableList = getMilvusElasticsearchFailedTableList();
        List<MilvusElasticsearchStatusEntry> resultList = new ArrayList<>();
        successTableList.forEach(table -> resultList.add(new MilvusElasticsearchStatusEntry(table, 0)));
        failedTableList.forEach(table -> resultList.add(new MilvusElasticsearchStatusEntry(table, 1)));
        return resultList;
    }

    private List<MigrationStatus> readMigrationStatusList(TaskWorkspace taskWorkspace) {
        String statusFilePath = MigrationStatusHelper.generateMigrationStatusFilePath(taskWorkspace);
        try {
            String statusContent = FileUtils.readFileContents(statusFilePath);
            return JSON.parseArray(statusContent, MigrationStatus.class);
        } catch (FileNotFoundException e) {
            return new ArrayList<>();
        } catch (IOException e) {
            throw new TaskException("Failed to read migration status file: " + statusFilePath, e);
        }
    }

    private void appendFullMigrationStatus(StringBuilder detailBuilder) {
        detailBuilder.append(System.lineSeparator()).append("[Full Migration]").append(System.lineSeparator());

        DatabaseType sourceDbType = TaskHelper.loadSourceDbType(taskWorkspace);
        if (DatabaseType.MILVUS.equals(sourceDbType) || DatabaseType.ELASTICSEARCH.equals(sourceDbType)) {
            appendMilvusElasticsearchStatus(detailBuilder);
            return;
        }

        FullTotalInfo fullTotalInfo = getFullTotalInfo();
        String statusModel = "Total migration data: %s MB%s"
                + "Total migration records: %s%s"
                + "Migration speed: %s MB/s%s"
                + "Migration duration: %s:%s:%s";
        if (fullTotalInfo != null) {
            int time = fullTotalInfo.getTime();
            detailBuilder.append(String.format(statusModel, fullTotalInfo.getData(), System.lineSeparator(),
                    fullTotalInfo.getRecord(), System.lineSeparator(), fullTotalInfo.getSpeed(), System.lineSeparator(),
                    time / 3600, (time % 3600) / 60, time % 60));
        } else {
            detailBuilder.append(String.format(statusModel, "0", System.lineSeparator(), "0", System.lineSeparator(),
                    "0", System.lineSeparator(), "0", "0", "0"));
        }
        detailBuilder.append(System.lineSeparator());
    }

    private void appendMilvusElasticsearchStatus(StringBuilder detailBuilder) {
        List<String> successTableList = getMilvusElasticsearchSuccessTableList();
        List<String> failedTableList = getMilvusElasticsearchFailedTableList();

        detailBuilder.append("Success migration objects: ")
                .append(successTableList.size())
                .append(System.lineSeparator());
        detailBuilder.append("Failed migration objects: ")
                .append(failedTableList.size())
                .append(System.lineSeparator());
    }

    private void appendFullCheckStatus(StringBuilder detailBuilder) {
        detailBuilder.append(System.lineSeparator()).append("[Full Data Check]").append(System.lineSeparator());

        int failedCount = getCheckFailProgress().size();
        int successCount = getCheckProgress().size();
        String statusModel = "Total check tables: %s%s"
                + "Success check tables: %s%s"
                + "Failed check tables: %s";
        detailBuilder.append(String.format(statusModel, successCount + failedCount, System.lineSeparator(),
                successCount, System.lineSeparator(), failedCount)).append(System.lineSeparator());
    }

    private void appendIncrementalMigrationStatus(StringBuilder detailBuilder) {
        detailBuilder.append(System.lineSeparator()).append("[Incremental Migration]").append(System.lineSeparator());

        Optional<IncrementalAndReverseEntry> optional = getIncrementalProgress();
        String statusModel = "Total migration records: %s%s"
                + "Success records: %s%s"
                + "Failed records: %s%s"
                + "Skipped records: %s%s"
                + "Migration speed: %s records/s";
        if (optional.isPresent()) {
            IncrementalAndReverseEntry incrementalProgress = optional.get();
            detailBuilder.append(String.format(statusModel, incrementalProgress.getCount(), System.lineSeparator(),
                    incrementalProgress.getSuccessCount(), System.lineSeparator(),
                    incrementalProgress.getFailCount(), System.lineSeparator(),
                    incrementalProgress.getSkippedCount(), System.lineSeparator(),
                    incrementalProgress.getSinkSpeed()));
        } else {
            detailBuilder.append(String.format(statusModel, "0", System.lineSeparator(), "0", System.lineSeparator(),
                    "0", System.lineSeparator(), "0", System.lineSeparator(), "0"));
        }
        detailBuilder.append(System.lineSeparator());
    }

    private void appendReverseMigrationStatus(StringBuilder detailBuilder) {
        detailBuilder.append(System.lineSeparator()).append("[Reverse Migration]").append(System.lineSeparator());

        Optional<IncrementalAndReverseEntry> optional = getReverseProgress();
        String statusModel = "Total migration records: %s%s"
                + "Success records: %s%s"
                + "Failed records: %s%s"
                + "Skipped records: %s%s"
                + "Migration speed: %s records/s";
        if (optional.isPresent()) {
            IncrementalAndReverseEntry reverseEntry = optional.get();
            detailBuilder.append(String.format(statusModel, reverseEntry.getCount(), System.lineSeparator(),
                    reverseEntry.getSuccessCount(), System.lineSeparator(),
                    reverseEntry.getFailCount(), System.lineSeparator(),
                    reverseEntry.getSkippedCount(), System.lineSeparator(),
                    reverseEntry.getSinkSpeed()));
        } else {
            detailBuilder.append(String.format(statusModel, "0", System.lineSeparator(), "0", System.lineSeparator(),
                    "0", System.lineSeparator(), "0", System.lineSeparator(), "0"));
        }
        detailBuilder.append(System.lineSeparator());
    }

    private List<String> getMilvusElasticsearchSuccessTableList() {
        String successFilePath = MigrationStatusHelper.generateFullSuccessTableStatusFilePath(taskWorkspace);
        try {
            return FileUtils.readFileContents(successFilePath).trim().lines().collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            return new ArrayList<>();
        } catch (IOException e) {
            throw new TaskException("Failed to read migration progress file: " + successFilePath, e);
        }
    }

    private List<String> getMilvusElasticsearchFailedTableList() {
        String failedFilePath = MigrationStatusHelper.generateFullFailedTableStatusFilePath(taskWorkspace);
        try {
            return FileUtils.readFileContents(failedFilePath).trim().lines().collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            return new ArrayList<>();
        } catch (IOException e) {
            throw new TaskException("Failed to read migration progress file: " + failedFilePath, e);
        }
    }

    private FullTotalInfo getFullTotalInfo() {
        String statusPath = MigrationStatusHelper.generateFullTotalInfoStatusFilePath(taskWorkspace);
        try {
            String contents = FileUtils.readFileContents(statusPath);
            return JSON.parseObject(contents, FullTotalInfo.class);
        } catch (IOException e) {
            throw new PortalException("Failed to read full total info file: " + statusPath, e);
        }
    }

    private List<FullEntry> getFullTableProgress() {
        String tableStatusFilePath = MigrationStatusHelper.generateFullTableStatusFilePath(taskWorkspace);
        return readFullMigrationProgress(tableStatusFilePath);
    }

    private List<FullEntry> getFullViewProgress() {
        String viewStatusFilePath = MigrationStatusHelper.generateFullViewStatusFilePath(taskWorkspace);
        return readFullMigrationProgress(viewStatusFilePath);
    }

    private List<FullEntry> getFullFunctionProgress() {
        String functionStatusFilePath = MigrationStatusHelper.generateFullFuncStatusFilePath(taskWorkspace);
        return readFullMigrationProgress(functionStatusFilePath);
    }

    private List<FullEntry> getFullTriggerProgress() {
        String triggerStatusFilePath = MigrationStatusHelper.generateFullTriggerStatusFilePath(taskWorkspace);
        return readFullMigrationProgress(triggerStatusFilePath);
    }

    private List<FullEntry> getFullProcedureProgress() {
        String procedureStatusFilePath = MigrationStatusHelper.generateFullProcStatusFilePath(taskWorkspace);
        return readFullMigrationProgress(procedureStatusFilePath);
    }

    private List<FullEntry> readFullMigrationProgress(String statusFilePath) {
        try {
            if (FileUtils.checkFileExists(statusFilePath)) {
                String contents = FileUtils.readFileContents(statusFilePath);
                List<FullEntry> fullEntries = JSON.parseArray(contents, FullEntry.class);
                if (fullEntries != null && !fullEntries.isEmpty()) {
                    return fullEntries;
                }
            }
            return new ArrayList<>();
        } catch (IOException e) {
            throw new PortalException("Failed to read full migration progress file: " + statusFilePath, e);
        }
    }

    private List<CheckFailEntry> getCheckFailProgress() {
        String statusPath = MigrationStatusHelper.generateFullCheckFailedObjectStatusFilePath(taskWorkspace);
        try {
            if (FileUtils.checkFileExists(statusPath)) {
                String contents = FileUtils.readFileContents(statusPath);
                List<CheckFailEntry> failEntries = JSON.parseArray(contents, CheckFailEntry.class);
                if (failEntries != null && !failEntries.isEmpty()) {
                    return failEntries;
                }
            }
            return new ArrayList<>();
        } catch (IOException e) {
            throw new PortalException("Failed to read check fail progress file: " + statusPath, e);
        }
    }

    private List<CheckEntry> getCheckProgress() {
        String statusPath = MigrationStatusHelper.generateFullCheckSuccessObjectStatusFilePath(taskWorkspace);
        try {
            if (FileUtils.checkFileExists(statusPath)) {
                String contents = FileUtils.readFileContents(statusPath);
                List<CheckEntry> successEntries = JSON.parseArray(contents, CheckEntry.class);
                if (successEntries != null && !successEntries.isEmpty()) {
                    return successEntries;
                }
            }
            return new ArrayList<>();
        } catch (IOException e) {
            throw new PortalException("Failed to read check progress file: " + statusPath, e);
        }
    }

    private Optional<IncrementalAndReverseEntry> getIncrementalProgress() {
        String statusPath = MigrationStatusHelper.generateIncrementalStatusFilePath(taskWorkspace);
        try {
            if (FileUtils.checkFileExists(statusPath)) {
                String contents = FileUtils.readFileContents(statusPath);
                return Optional.of(JSON.parseObject(contents, IncrementalAndReverseEntry.class));
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new PortalException("Failed to read incremental progress file: " + statusPath, e);
        }
    }

    private Optional<IncrementalAndReverseEntry> getReverseProgress() {
        String statusPath = MigrationStatusHelper.generateReverseStatusFilePath(taskWorkspace);
        try {
            if (FileUtils.checkFileExists(statusPath)) {
                String contents = FileUtils.readFileContents(statusPath);
                return Optional.of(JSON.parseObject(contents, IncrementalAndReverseEntry.class));
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new PortalException("Failed to read reverse progress file: " + statusPath, e);
        }
    }
}
