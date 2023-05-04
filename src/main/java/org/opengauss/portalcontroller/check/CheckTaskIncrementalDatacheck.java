package org.opengauss.portalcontroller.check;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.Datacheck;
import org.opengauss.portalcontroller.software.Kafka;
import org.opengauss.portalcontroller.software.Software;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.Plan.runningTaskList;

/**
 * The type Check task incremental datacheck.
 */
public class CheckTaskIncrementalDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskIncrementalDatacheck.class);
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

    @Override
    public boolean installAllPackages(boolean download) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new Datacheck());
        boolean flag = InstallMigrationTools.installMigrationTools(softwareArrayList, download);
        return flag;
    }

    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskIncrementalDatacheck();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.CHECK);
        return flag;
    }

    @Override
    public void copyConfigFiles(String workspaceId) {

    }

    @Override
    public void prepareWork(String workspaceId) {
        runningTaskList.add(Command.Start.Mysql.FULL_CHECK);
        Task.startTaskMethod(Method.Run.ZOOKEEPER, 8000, "");
        Task.startTaskMethod(Method.Run.KAFKA, 8000, "");
        Task.startTaskMethod(Method.Run.REGISTRY, 8000, "");
        changeParameters(workspaceId);
    }

    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSinglePropertiesParameter("dataDir", hashtable.get(Debezium.Zookeeper.TMP_PATH), hashtable.get(Debezium.Zookeeper.CONFIG_PATH));
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        kafkaConfigTable.put("log.dirs", hashtable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        Tools.changePropertiesParameters(kafkaConfigTable, hashtable.get(Debezium.Kafka.CONFIG_PATH));
        Tools.changeSinglePropertiesParameter("offset.storage.file.filename", PathUtils.combainPath(true, PortalControl.portalControlPath + "tmp", "connect.offsets"), hashtable.get(Debezium.Connector.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.INCREMENTAL), hashtable.get(Check.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Sink.CONFIG_PATH));
        String sourceTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sourceTopic, hashtable.get(Check.Source.CONFIG_PATH));
        String sinkTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sinkTopic, hashtable.get(Check.Sink.CONFIG_PATH));
    }

    @Override
    public void start(String workspaceId) {
        Task.startTaskMethod(Method.Run.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK, 15000, "Started CheckApplication in");
        checkEnd();
    }

    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration) {
            LOGGER.info("Incremental migration is running...");
            if (!Tools.outputDatacheckStatus(Parameter.CHECK_INCREMENTAL)) {
                break;
            }
            Tools.sleepThread(1000, "running incremental migraiton datacheck");
        }
        if (Plan.stopIncrementalMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.INCREMENTAL_MIGRATION_FINISHED;
                Plan.pause = true;
                Tools.sleepThread(50, "pausing the plan");
            }
            if (PortalControl.taskList.contains("start mysql reverse migration")) {
                try {
                    PgConnection conn = JdbcTools.getPgConnection();
                    JdbcTools.changeAllTable(conn);
                    String slotName = "slot_" + Plan.workspaceId;
                    JdbcTools.createLogicalReplicationSlot(conn, slotName);
                    conn.close();
                } catch (SQLException e) {
                    PortalException portalException = new PortalException("SQL exception", "select global variable", e.getMessage());
                    portalException.setRequestInformation("Create slot failed.");
                    PortalControl.refuseReverseMigrationReason = portalException.getMessage();
                    portalException.printLog(LOGGER);
                }
            }
            Task.stopTaskMethod(Method.Run.CHECK);
            Task.stopTaskMethod(Method.Run.CHECK_SINK);
            Task.stopTaskMethod(Method.Run.CHECK_SOURCE);
            Task.stopTaskMethod(Method.Run.CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.CONNECT_SOURCE);
        }
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.PATH));
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
