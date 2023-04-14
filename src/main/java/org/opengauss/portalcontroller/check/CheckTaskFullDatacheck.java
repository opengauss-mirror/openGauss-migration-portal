package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.*;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.Kafka;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Check task full datacheck.
 */
public class CheckTaskFullDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskFullDatacheck.class);
    private String workspaceId = "";

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
     * Install datacheck package.
     */
    @Override
    public boolean installAllPackages(boolean download) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        boolean flag = InstallMigrationTools.installMigrationTools(softwareArrayList, download);
        return flag;
    }

    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskFullDatacheck();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.CHECK);
        return flag;
    }

    /**
     * Copy datacheck config files.
     */
    @Override
    public void copyConfigFiles(String workspaceId) {
    }

    @Override
    public void prepareWork(String workspaceId) {

    }

    /**
     * Change datacheck parameters.
     */
    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSinglePropertiesParameter("dataDir", hashtable.get(Debezium.Zookeeper.TMP_PATH), hashtable.get(Debezium.Zookeeper.CONFIG_PATH));
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        kafkaConfigTable.put("log.dirs", hashtable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        Tools.changePropertiesParameters(kafkaConfigTable, hashtable.get(Debezium.Kafka.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", false, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", false, hashtable.get(Check.Sink.CONFIG_PATH));
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.FULL), hashtable.get(Check.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
    }

    @Override
    public void start(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION_CHECK;
        }
        Plan.runningTaskList.add(Command.Start.Mysql.FULL_CHECK);
        Task.startTaskMethod(Method.Run.ZOOKEEPER, 8000, "");
        Task.startTaskMethod(Method.Run.KAFKA, 8000, "");
        changeParameters(workspaceId);
        Task.startTaskMethod(Method.Run.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK, 15000, "Started CheckApplication in");
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_FULL_MIGRATION_CHECK;
        }
        checkEnd();
    }


    /**
     * Check necessary process exist boolean.
     *
     * @return the boolean
     */
    public boolean checkNecessaryProcessExist() {
        boolean flag = false;
        boolean flag1 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.ZOOKEEPER)) != -1;
        boolean flag2 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.KAFKA)) != -1;
        flag = flag1 && flag2;
        return flag;
    }

    public void checkEnd() {
        while (!Plan.stopPlan) {
            if (Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CHECK)) == -1) {
                if (PortalControl.status != Status.ERROR) {
                    LOGGER.info("Full migration datacheck is finished.");
                    PortalControl.status = Status.FULL_MIGRATION_CHECK_FINISHED;
                }
                break;
            }
            if (!Tools.outputDatacheckStatus(Parameter.CHECK_FULL)) {
                break;
            }
            Tools.sleepThread(1000, "running full migration datacheck");
        }
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
