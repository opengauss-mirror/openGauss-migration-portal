package org.opengauss.portalcontroller.check;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.software.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Check task incremental migration.
 */
public class CheckTaskIncrementalMigration implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskIncrementalMigration.class);

    @Override
    public boolean installAllPackages(boolean download) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new ConnectorMysql());
        boolean flag = InstallMigrationTools.installMigrationTools(softwareArrayList, download);
        return flag;
    }

    /**
     * Install incremental migration tools package.
     */
    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskIncrementalMigration();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.INCREMENTAL_MIGRATION);
        return flag;
    }

    /**
     * Copy incremental migration tools files.
     */
    public void copyConfigFiles(String workspaceId) {

    }

    /**
     * Change incremental migration tools parameters.
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
        Hashtable<String, String> hashtable1 = new Hashtable<>();
        hashtable1.put("name", "mysql-source-" + workspaceId);
        hashtable1.put("database.server.name", "mysql_server_" + workspaceId);
        hashtable1.put("database.history.kafka.topic", "mysql_server_" + workspaceId + "_history");
        hashtable1.put("transforms.route.regex", "^" + "mysql_server_" + workspaceId + "(.*)");
        hashtable1.put("transforms.route.replacement", "mysql_server_" + workspaceId + "_topic");
        String incrementalFolder = hashtable.get(Status.INCREMENTAL_FOLDER);
        hashtable1.put("source.process.file.path", incrementalFolder);
        Tools.changePropertiesParameters(hashtable1, hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        Hashtable<String, String> hashtable2 = new Hashtable<>();
        hashtable2.put("name", "mysql-sink-" + workspaceId);
        hashtable2.put("topics", "mysql_server_" + workspaceId + "_topic");
        Tools.createFile(incrementalFolder, false);
        hashtable2.put("sink.process.file.path", incrementalFolder);
        hashtable2.put("xlog.location", hashtable.get(Status.XLOG_PATH));
        Tools.changePropertiesParameters(hashtable2, hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
    }

    @Override
    public void prepareWork(String workspaceId) {
        Tools.changeIncrementalMigrationParameters(PortalControl.toolsMigrationParametersTable);
        changeParameters(workspaceId);
        if (!checkNecessaryProcessExist()) {
            Task.startTaskMethod(Method.Run.ZOOKEEPER, 8000, "");
            Task.startTaskMethod(Method.Run.KAFKA, 8000, "");
            Task.startTaskMethod(Method.Run.REGISTRY, 8000, "");
        }
        if (checkAnotherConnectExists()) {
            LOGGER.error("Another connector is running.Cannot run incremental migration whose workspace id is " + workspaceId + " .");
            return;
        }
        Tools.findOffset();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeConnectXmlFile(workspaceId + "_source", hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        String standaloneSourcePath = hashtable.get(Debezium.Source.CONNECTOR_PATH);
        int sourcePort = StartPort.REST_MYSQL_SOURCE + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sourcePort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), standaloneSourcePath);
        Task.startTaskMethod(Method.Run.CONNECT_SOURCE, 8000, "");
    }

    @Override
    public void start(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_INCREMENTAL_MIGRATION;
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String standaloneSinkFilePath = hashtable.get(Debezium.Sink.CONNECTOR_PATH);
        Tools.changeConnectXmlFile(workspaceId + "_sink", hashtable.get(Debezium.Connector.LOG_PATTERN_PATH));
        int sinkPort = StartPort.REST_MYSQL_SINK + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sinkPort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), standaloneSinkFilePath);
        Task.startTaskMethod(Method.Run.CONNECT_SINK, 8000, "");
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_INCREMENTAL_MIGRATION;
        }
        checkEnd();
    }

    @Override
    public void checkEnd() {
        LOGGER.info("Incremental migration is running...");
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration && !PortalControl.taskList.contains("start mysql incremental migration datacheck")) {
            Tools.sleepThread(1000, "running incremental migraiton");
        }
        if (Plan.stopIncrementalMigration) {
            Task task = new Task();
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
                    LOGGER.error("SQL exception occurred in create logical replication slot.");
                }
            }
            task.stopTaskMethod(Method.Run.CONNECT_SINK);
            task.stopTaskMethod(Method.Run.CONNECT_SOURCE);
            LOGGER.info("Incremental migration stopped.");
        }
    }

    /**
     * Check another connect exists boolean.
     *
     * @return the boolean
     */
    public boolean checkAnotherConnectExists() {
        boolean flag = false;
        boolean flag1 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SOURCE)) != -1;
        boolean flag2 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REVERSE_CONNECT_SINK)) != -1;
        boolean flag3 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SOURCE)) != -1;
        boolean flag4 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CONNECT_SINK)) != -1;
        flag = flag1 || flag2 || flag3 || flag4;
        return flag;
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
        boolean flag3 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REGISTRY)) != -1;
        flag = flag1 && flag2 && flag3;
        return flag;
    }

    public void uninstall() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.PATH));
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
