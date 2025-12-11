/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import com.opencsv.CSVWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.Main;
import org.opengauss.config.Portal;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.PortalException;
import org.opengauss.migration.MigrationContext;
import org.opengauss.migration.helper.TaskHelper;
import org.opengauss.migration.status.StatusManager;
import org.opengauss.migration.status.model.MilvusElasticsearchStatusEntry;
import org.opengauss.migration.status.model.ObjectStatusEntry;
import org.opengauss.migration.workspace.TaskWorkspaceManager;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.PortUtils;
import org.opengauss.utils.ProcessUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Migration command receiver
 *
 * @since 2025/3/27
 */
public class MigrationCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(MigrationCommandReceiver.class);

    private final String taskId;

    public MigrationCommandReceiver(String taskId) {
        this.taskId = taskId;
    }

    /**
     * Start migration
     */
    public void start() {
        TaskWorkspaceManager workspaceManager = new TaskWorkspaceManager();
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);

        if (workspaceManager.isTaskRunning(taskWorkspace)) {
            LOGGER.error("Task {} is already running", taskId);
            return;
        }

        if (workspaceManager.checkTaskIdExists(taskId)) {
            MigrationContext.initialize(taskWorkspace);
            setQuarkusPort(taskWorkspace);
            Main.startQuarkus();
        } else {
            LOGGER.error("Task {} does not exist", taskId);
        }
    }

    /**
     * Stop incremental migration
     */
    public void stopIncremental() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "stopIncremental");
    }

    /**
     * Start reverse migration
     */
    public void startReverse() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "startReverse");
    }

    /**
     * Restart incremental migration
     */
    public void restartIncremental() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "restartIncremental");
    }

    /**
     * Restart reverse migration
     */
    public void restartReverse() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "restartReverse");
    }

    /**
     * Resume incremental migration
     */
    public void resumeIncremental() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "resumeIncremental");
    }

    /**
     * Resume reverse migration
     */
    public void resumeReverse() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "resumeReverse");
    }

    /**
     * Stop reverse migration
     */
    public void stopReverse() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "stopReverse");
    }

    /**
     * Stop migration
     */
    public void stop() {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskStopped(taskWorkspace)) {
            return;
        }
        sendRequest(taskWorkspace, "stop");
    }

    /**
     * Get migration status
     *
     * @param isDetail whether to print detailed information
     */
    public void status(boolean isDetail) {
        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        StatusManager statusManager = new StatusManager(taskWorkspace);
        if (!isDetail) {
            String status = statusManager.getStatus();
            LOGGER.info("Migration status: {}{}", System.lineSeparator(), status);
            return;
        }

        DatabaseType sourceDbType = TaskHelper.loadSourceDbType(taskWorkspace);

        if (DatabaseType.MILVUS.equals(sourceDbType) || DatabaseType.ELASTICSEARCH.equals(sourceDbType)) {
            List<MilvusElasticsearchStatusEntry> statusEntries = statusManager.getMilvusElasticsearchStatusEntryList();
            if (statusEntries.isEmpty()) {
                LOGGER.info("No detail migration status found");
            } else {
                exportMilvusElasticsearchCsv(statusEntries);
            }
            return;
        }

        List<ObjectStatusEntry> statusEntryList;
        if (DatabaseType.MYSQL.equals(sourceDbType)) {
            statusEntryList = statusManager.getMysqlObjectStatusEntryList();
        } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
            statusEntryList = statusManager.getPgsqlObjectStatusEntryList();
        } else {
            LOGGER.error("Unsupported database type: {}", sourceDbType);
            return;
        }

        if (statusEntryList.isEmpty()) {
            LOGGER.info("No detail migration status found");
        } else {
            exportCsv(statusEntryList);
        }
    }

    private void exportMilvusElasticsearchCsv(List<MilvusElasticsearchStatusEntry> statusEntryList) {
        String csvFilePath = String.format("%s/task_%s_status.csv", Portal.getInstance().getPortalTmpDirPath(), taskId);
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
            String[] header = {"Object name", "Status"};
            writer.writeNext(header);

            ArrayList<String[]> rows = new ArrayList<>();
            for (MilvusElasticsearchStatusEntry statusEntry : statusEntryList) {
                rows.add(new String[] {
                        statusEntry.getName(),
                        statusEntry.getStatus() == 0 ? "success" : "fail"
                });
            }
            writer.writeAll(rows);
            LOGGER.info("Export csv file successfully, file path: {}", csvFilePath);
        } catch (IOException e) {
            LOGGER.error("Failed to export csv file", e);
        }
    }

    private void exportCsv(List<ObjectStatusEntry> statusEntryList) {
        String csvFilePath = String.format("%s/task_%s_status.csv",
                Portal.getInstance().getPortalTmpDirPath(), taskId);
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
            String[] header = {
                "Schema", "Name", "Type", "Status(1 - pending, 2 - migrating, 3,4,5 - completed, 6,7 - failed)",
                "Percent", "Migration error", "Check Status(0 - success, 1 - fail)", "Check Message", "Repair File Path"
            };
            writer.writeNext(header);

            ArrayList<String[]> rows = new ArrayList<>();
            for (ObjectStatusEntry statusEntry : statusEntryList) {
                rows.add(new String[] {
                        statusEntry.getSchema(),
                        statusEntry.getName(),
                        statusEntry.getType(),
                        String.valueOf(statusEntry.getStatus()),
                        String.valueOf(statusEntry.getPercent()),
                        statusEntry.getError(),
                        statusEntry.getCheckStatus() == null ? "" : String.valueOf(statusEntry.getCheckStatus()),
                        statusEntry.getCheckMessage(),
                        statusEntry.getRepairFilePath()
                });
            }
            writer.writeAll(rows);
            LOGGER.info("Export csv file successfully, file path: {}", csvFilePath);
        } catch (IOException e) {
            LOGGER.error("Failed to export csv file", e);
        }
    }

    private void sendRequest(TaskWorkspace taskWorkspace, String api) {
        String curl = String.format(Locale.ROOT, "curl -X POST http://localhost:%d/task/%s",
                readQuarkusPort(taskWorkspace), api);
        try {
            String curlResult = ProcessUtils.executeCommandWithResult(curl);
            if (curlResult != null && curlResult.contains("SUCCESS")) {
                LOGGER.info("Task {} {} command was sent successfully. For detail, please refer to the main "
                        + "migration process log.", taskId, api);
            } else {
                LOGGER.error("Task {} {} command was sent failed, response: {}{}",
                        taskId, api, System.lineSeparator(), curlResult);
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Execute curl command failed, command: {}", curl, e);
        }
    }

    private int readQuarkusPort(TaskWorkspace taskWorkspace) {
        try {
            String portFilePath = taskWorkspace.getQuarkusPortFilePath();
            return Integer.parseInt(FileUtils.readFileContents(portFilePath).trim());
        } catch (IOException e) {
            throw new PortalException("Failed to read quarkus port from file", e);
        } catch (NumberFormatException e) {
            throw new PortalException("Port is not a number in port file, please restart migration", e);
        }
    }

    private boolean isTaskStopped(TaskWorkspace taskWorkspace) {
        TaskWorkspaceManager workspaceManager = new TaskWorkspaceManager();
        if (!workspaceManager.isTaskRunning(taskWorkspace)) {
            LOGGER.error("Task {} is already stopped", taskId);
            return true;
        }
        return false;
    }

    private void setQuarkusPort(TaskWorkspace taskWorkspace) {
        try {
            String quarkusPort = System.getProperty("quarkus.http.port");
            if (quarkusPort == null) {
                int expectedPort = 6000;
                quarkusPort = String.valueOf(PortUtils.getUsefulPort(expectedPort));
            }
            System.setProperty("quarkus.http.port", quarkusPort);

            String portFilePath = taskWorkspace.getQuarkusPortFilePath();
            FileUtils.deletePath(portFilePath);
            FileUtils.writeToFile(portFilePath, quarkusPort, false);
            FileUtils.setFileReadOnly(portFilePath);
        } catch (SocketException e) {
            throw new PortalException("Can not get useful port used as quarkus port", e);
        } catch (IOException e) {
            throw new PortalException("Failed to write quarkus port to file", e);
        }
    }
}
