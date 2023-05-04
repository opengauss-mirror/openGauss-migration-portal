package org.opengauss.portalcontroller.status;

import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.Plan;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.RuntimeExecTools;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;

/**
 * The type Thread status controller.
 */
public class ThreadStatusController extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadStatusController.class);
    private String workspaceId;

    /**
     * Gets workspace id.
     *
     * @return the workspace id
     */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /**
     * Sets workspace id.
     *
     * @param workspaceId the workspace id
     */
    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    /**
     * The Exit.
     */
    public boolean exit = false;

    /**
     * The constant fullMigrationStatus.
     */
    public static FullMigrationStatus fullMigrationStatus = new FullMigrationStatus();

    /**
     * The constant portalStatusWriterArrayList.
     */
    public static ArrayList<PortalStatusWriter> portalStatusWriterArrayList = new ArrayList<>();

    @Override
    public void run() {
        while (!exit) {
            int time = 0;
            ChangeStatusTools.writePortalStatus();
            Hashtable<String, String> toolsConfigHashtable = PortalControl.toolsConfigParametersTable;
            String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
            String path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_init_replica.json";
            if (new File(path).exists()) {
                ChangeStatusTools.changeFullStatus();
            }
            if (PortalControl.status < Status.START_REVERSE_MIGRATION && PortalControl.status > Status.FULL_MIGRATION_CHECK_FINISHED) {
                String sourceIncrementalStatusPath = "";
                String sinkIncrementalStatusPath = "";
                File directory = new File(toolsConfigHashtable.get(Status.INCREMENTAL_FOLDER));
                if (directory.exists() && directory.isDirectory()) {
                    for (File file : Objects.requireNonNull(directory.listFiles())) {
                        if (file.getName().contains("forward-source-process")) {
                            sourceIncrementalStatusPath = file.getAbsolutePath();
                        } else if (file.getName().contains("forward-sink-process")) {
                            sinkIncrementalStatusPath = file.getAbsolutePath();
                        }
                    }
                }
                String incrementalStatusPath = toolsConfigHashtable.get(Status.INCREMENTAL_PATH);
                if (new File(sourceIncrementalStatusPath).exists() && new File(sinkIncrementalStatusPath).exists()) {
                    time = ChangeStatusTools.changeIncrementalStatus(sourceIncrementalStatusPath, sinkIncrementalStatusPath, incrementalStatusPath, "createCount");
                }
            }
            if (PortalControl.status >= Status.START_REVERSE_MIGRATION && PortalControl.status != Status.ERROR) {
                String sourceReverseStatusPath = PathUtils.combainPath(true, toolsConfigHashtable.get(Status.REVERSE_FOLDER), "reverse-source-process.txt");
                String sinkReverseStatusPath = PathUtils.combainPath(true, toolsConfigHashtable.get(Status.REVERSE_FOLDER), "reverse-sink-process.txt");
                File directory = new File(toolsConfigHashtable.get(Status.REVERSE_FOLDER));
                if (directory.exists() && directory.isDirectory()) {
                    for (File file : Objects.requireNonNull(directory.listFiles())) {
                        if (file.getName().contains("reverse-source-process")) {
                            sourceReverseStatusPath = file.getAbsolutePath();
                        } else if (file.getName().contains("reverse-sink-process")) {
                            sinkReverseStatusPath = file.getAbsolutePath();
                        }
                    }
                }
                String reverseStatusPath = toolsConfigHashtable.get(Status.REVERSE_PATH);
                if (new File(sourceReverseStatusPath).exists() && new File(sinkReverseStatusPath).exists()) {
                    time = ChangeStatusTools.changeIncrementalStatus(sourceReverseStatusPath, sinkReverseStatusPath, reverseStatusPath, "count");
                }
            }
            try {
                String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
                String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
                Hashtable<String, String> hashtable = new Hashtable<>();
                hashtable.put(PathUtils.combainPath(true, kafkaPath + "logs", "server.log"), toolsConfigHashtable.get(Debezium.LOG_PATH) + "server.log");
                hashtable.put(PathUtils.combainPath(true, confluentPath + "logs", "schema-registry.log"), toolsConfigHashtable.get(Debezium.LOG_PATH) + "schema-registry.log");
                for (String key : hashtable.keySet()) {
                    if (new File(key).exists()) {
                        RuntimeExecTools.copyFile(key, hashtable.get(key), true);
                    }
                }
                File logFile = new File(confluentPath + "logs");
                if (logFile.exists() && logFile.isDirectory()) {
                    File[] logFileList = logFile.listFiles();
                    String workspaceDebeziumLogPath = toolsConfigHashtable.get(Debezium.LOG_PATH);
                    for (File file : logFileList) {
                        RuntimeExecTools.copyFileStartWithWord(file, workspaceDebeziumLogPath, "connect_" + workspaceId + "_source.log", "connect_source.log", true);
                        RuntimeExecTools.copyFileStartWithWord(file, workspaceDebeziumLogPath, "connect_" + workspaceId + "_sink.log", "connect_sink.log", true);
                        RuntimeExecTools.copyFileStartWithWord(file, workspaceDebeziumLogPath, "connect_" + workspaceId + "_reverse_source.log", "reverse_connect_source.log", true);
                        RuntimeExecTools.copyFileStartWithWord(file, workspaceDebeziumLogPath, "connect_" + workspaceId + "_reverse_sink.log", "reverse_connect_sink.log", true);
                    }
                }
            } catch (PortalException e) {
                e.setRequestInformation("Cannot find logs.");
                e.shutDownPortal(LOGGER);
            }
            if (1000 - time > 0) {
                Tools.sleepThread(1000 - time, "writing the status");
            }
        }
    }
}
