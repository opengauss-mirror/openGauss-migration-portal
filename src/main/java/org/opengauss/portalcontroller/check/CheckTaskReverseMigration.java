package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.software.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Check task reverse migration.
 */
public class CheckTaskReverseMigration implements CheckTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskReverseMigration.class);

    @Override
    public boolean installAllPackages(boolean download) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new ConnectorOpengauss());
        boolean flag = InstallMigrationTools.installMigrationTools(softwareArrayList, download);
        return flag;
    }

    /**
     * Install incremental migration tools package.
     */
    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskReverseMigration();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.REVERSE_MIGRATION);
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
        Tools.changeReverseMigrationParameters(PortalControl.toolsMigrationParametersTable);
        String sourceConfigPath = hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH);
        String sinkConfigPath = hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        Hashtable<String, String> hashtable1 = new Hashtable<>();
        hashtable1.put("database.server.name", "opengauss_server_" + workspaceId);
        hashtable1.put("database.history.kafka.topic", "opengauss_server_" + workspaceId + "_history");
        hashtable1.put("transforms.route.regex", "^" + "opengauss_server_" + workspaceId + "(.*)");
        hashtable1.put("transforms.route.replacement", "opengauss_server_" + workspaceId + "_topic");
        hashtable1.put("source.process.file.path", hashtable.get(Status.REVERSE_FOLDER));
        hashtable1.put("slot.name", Plan.slotName);
        Tools.changePropertiesParameters(hashtable1, sourceConfigPath);
        Hashtable<String, String> hashtable2 = new Hashtable<>();
        hashtable2.put("topics", "opengauss_server_" + workspaceId + "_topic");
        hashtable2.put("sink.process.file.path", hashtable.get(Status.REVERSE_FOLDER));
        Tools.changePropertiesParameters(hashtable2, sinkConfigPath);
        Tools.setXLogPath();
    }

    @Override
    public void prepareWork(String workspaceId) {
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_REVERSE_MIGRATION;
        }
        Tools.changeIncrementalMigrationParameters(PortalControl.toolsMigrationParametersTable);
        changeParameters(workspaceId);
        if (!checkNecessaryProcessExist()) {
            Task.startTaskMethod(Method.Run.ZOOKEEPER, 8000, "");
            Task.startTaskMethod(Method.Run.KAFKA, 8000, "");
            Task.startTaskMethod(Method.Run.REGISTRY, 8000, "");
        }
    }

    @Override
    public void start(String workspaceId) {
        if (checkAnotherConnectExists()) {
            LOGGER.error("Another connector is running.Cannot run reverse migration with workspaceId is " + workspaceId + " .");
            return;
        }
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        int sourcePort = StartPort.REST_OPENGAUSS_SOURCE + PortalControl.portId * 10;
        int port = Tools.getAvailablePorts(sourcePort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port), hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH));
        String confluentPath = hashtable.get(Debezium.Confluent.PATH);
        Tools.changeConnectXmlFile(workspaceId + "_reverse_source", confluentPath + "etc/kafka/connect-log4j.properties");
        Task.startTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE, 8000, "");
        int sinkPort = StartPort.REST_OPENGAUSS_SINK + PortalControl.portId * 10;
        int port2 = Tools.getAvailablePorts(sinkPort, 1, 1000).get(0);
        Tools.changeSinglePropertiesParameter("rest.port", String.valueOf(port2), hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH));
        Tools.changeConnectXmlFile(workspaceId + "_reverse_sink", confluentPath + "etc/kafka/connect-log4j.properties");
        Task.startTaskMethod(Method.Run.REVERSE_CONNECT_SINK, 8000, "");
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_REVERSE_MIGRATION;
        }
        checkEnd();
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

    @Override
    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration && !PortalControl.taskList.contains("start mysql reverse migration datacheck")) {
            LOGGER.info("Reverse migration is running...");
            Tools.sleepThread(1000, "running reverse migraiton");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                Tools.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
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
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationTools.removeSingleMigrationToolFiles(filePaths, errorPath);
    }
}
