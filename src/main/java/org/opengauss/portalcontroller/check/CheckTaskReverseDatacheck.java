package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.*;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.software.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.Plan.runningTaskList;

/**
 * The type Check task reverse datacheck.
 */
public class CheckTaskReverseDatacheck implements CheckTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckTaskReverseDatacheck.class);

    @Override
    public boolean installAllPackages(boolean download) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Kafka());
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new Datacheck());
        boolean flag = InstallMigrationTools.installMigrationTools(softwareArrayList, download);
        return flag;
    }

    /**
     * Install datacheck package.
     */
    @Override
    public boolean installAllPackages() {
        CheckTask checkTask = new CheckTaskReverseDatacheck();
        boolean flag = InstallMigrationTools.installSingleMigrationTool(checkTask, MigrationParameters.Install.CHECK);
        return flag;
    }

    /**
     * Copy datacheck config files.
     */
    @Override
    public void copyConfigFiles(String workspaceId) {

    }

    /**
     * Change datacheck parameters.
     */
    @Override
    public void changeParameters(String workspaceId) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String kafkaPath = hashtable.get(Debezium.Kafka.PATH);
        Tools.changeSinglePropertiesParameter("dataDir", hashtable.get(Debezium.Zookeeper.TMP_PATH), hashtable.get(Debezium.Zookeeper.CONFIG_PATH));
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        kafkaConfigTable.put("log.dirs", hashtable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        Tools.changePropertiesParameters(kafkaConfigTable, hashtable.get(Debezium.Kafka.CONFIG_PATH));
        Tools.changeMigrationDatacheckParameters(PortalControl.toolsMigrationParametersTable);
        Tools.changeSingleYmlParameter("data.check.data-path", hashtable.get(Check.Result.REVERSE), hashtable.get(Check.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Source.CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-enable", true, hashtable.get(Check.Sink.CONFIG_PATH));
        String sourceTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sourceTopic, hashtable.get(Check.Source.CONFIG_PATH));
        String sinkTopic = Tools.getSinglePropertiesParameter("transforms.route.replacement", hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH));
        Tools.changeSingleYmlParameter("spring.extract.debezium-topic", sinkTopic, hashtable.get(Check.Sink.CONFIG_PATH));
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
    public void start(String workspaceId) {
        Task.startTaskMethod(Method.Run.CHECK_SOURCE, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK_SINK, 15000, "Started ExtractApplication in");
        Task.startTaskMethod(Method.Run.CHECK, 15000, "Started CheckApplication in");
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
        boolean flag3 = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.REGISTRY)) != -1;
        flag = flag && flag3;
        return flag;
    }

    public void checkEnd() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration) {
            LOGGER.info("Reverse migration is running...");
            if (!Tools.outputDatacheckStatus(Parameter.CHECK_REVERSE)) {
                break;
            }
            Tools.sleepThread(1000, "running reverse migraiton datacheck");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                Tools.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.CHECK);
            Task.stopTaskMethod(Method.Run.CHECK_SINK);
            Task.stopTaskMethod(Method.Run.CHECK_SOURCE);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
        }
    }

    public void uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
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
