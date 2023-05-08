/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller;

import org.opengauss.portalcontroller.check.CheckTaskFullDatacheck;
import org.opengauss.portalcontroller.check.CheckTaskIncrementalDatacheck;
import org.opengauss.portalcontroller.check.CheckTaskIncrementalMigration;
import org.opengauss.portalcontroller.check.CheckTaskMysqlFullMigration;
import org.opengauss.portalcontroller.check.CheckTaskReverseDatacheck;
import org.opengauss.portalcontroller.check.CheckTaskReverseMigration;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

/**
 * Task
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 * The type Task.
 */
public class Task {
    private static HashMap<String, String> taskProcessMap = new HashMap<>();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Task.class);
    private static HashMap<String, String> taskLogMap = new HashMap<>();

    /**
     * The constant ALL_TASK_LIST.
     */
    public static final List<String> ALL_TASK_LIST = Arrays.asList(
            "start mysql full migration",
            "start mysql full migration datacheck",
            "start mysql incremental migration",
            "start mysql incremental migration datacheck",
            "start mysql reverse migration",
            "start mysql reverse migration datacheck"
    );

    /**
     * Gets task process map.
     *
     * @return the task process map
     */
    public static HashMap<String, String> getTaskProcessMap() {
        return Task.taskProcessMap;
    }

    /**
     * Sets task process map.
     *
     * @param map the map
     */
    public static void setTaskProcessMap(HashMap<String, String> map) {
        Task.taskProcessMap = map;
    }

    /**
     * The constant runTaskHandlerHashMap.
     */
    public static HashMap<String, PortalControl.MethodRunner> runTaskHandlerHashMap = new HashMap<>();
    /**
     * The constant stopTaskHandlerHashMap.
     */
    public static HashMap<String, PortalControl.EventHandler> stopTaskHandlerHashMap = new HashMap<>();

    /**
     * Gets task log map.
     *
     * @return the task log map
     */
    public static HashMap<String, String> getTaskLogMap() {
        return taskLogMap;
    }

    /**
     * Sets task log map.
     *
     * @param taskLogMap the task log map
     */
    public static void setTaskLogMap(HashMap<String, String> taskLogMap) {
        Task.taskLogMap = taskLogMap;
    }

    /**
     * Init task process map.
     */
    public static void initTaskProcessMap() {
        HashMap<String, String> tempTaskProcessMap = new HashMap<>();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String kafkaPath = hashtable.get(Debezium.Kafka.PATH);
        String confluentPath = hashtable.get(Debezium.Confluent.PATH);
        String datacheckPath = hashtable.get(Check.PATH);
        tempTaskProcessMap.put(Method.Run.ZOOKEEPER, "QuorumPeerMain " + PathUtils.combainPath(true, kafkaPath + "config", "zookeeper.properties"));
        tempTaskProcessMap.put(Method.Run.KAFKA, "Kafka " + PathUtils.combainPath(true, kafkaPath + "config", "server.properties"));
        tempTaskProcessMap.put(Method.Run.REGISTRY, "SchemaRegistryMain " + PathUtils.combainPath(true, confluentPath + "etc", "schema-registry", "schema-registry.properties"));
        tempTaskProcessMap.put(Method.Run.CONNECT_SOURCE, "ConnectStandalone " + hashtable.get(Debezium.Source.CONNECTOR_PATH) + " " + hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.CONNECT_SINK, "ConnectStandalone " + hashtable.get(Debezium.Sink.CONNECTOR_PATH) + " " + hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.REVERSE_CONNECT_SOURCE, "ConnectStandalone " + hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH) + " " + hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.REVERSE_CONNECT_SINK, "ConnectStandalone " + hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH) + " " + hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH));
        String extractName = hashtable.get(Check.EXTRACT_NAME);
        String checkName = hashtable.get(Check.CHECK_NAME);
        tempTaskProcessMap.put(Method.Run.CHECK_SOURCE, "java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + hashtable.get(Check.Source.CONFIG_PATH) + " -jar " + datacheckPath + extractName + " --source");
        tempTaskProcessMap.put(Method.Run.CHECK_SINK, "java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + hashtable.get(Check.Sink.CONFIG_PATH) + " -jar " + datacheckPath + extractName + " --sink");
        tempTaskProcessMap.put(Method.Run.CHECK, "java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + hashtable.get(Check.CONFIG_PATH) + " -jar " + datacheckPath + checkName);
        setTaskProcessMap(tempTaskProcessMap);
    }

    /**
     * Init task log map.
     */
    public static void initTaskLogMap() {
        HashMap<String, String> tempTaskLogMap = new HashMap<>();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        tempTaskLogMap.put(Method.Run.ZOOKEEPER, hashtable.get(Debezium.Zookeeper.LOG_PATH));
        tempTaskLogMap.put(Method.Run.KAFKA, hashtable.get(Debezium.Kafka.LOG_PATH));
        tempTaskLogMap.put(Method.Run.REGISTRY, hashtable.get(Debezium.Registry.LOG_PATH));
        tempTaskLogMap.put(Method.Run.CONNECT_SOURCE, hashtable.get(Debezium.Source.LOG_PATH));
        tempTaskLogMap.put(Method.Run.CONNECT_SINK, hashtable.get(Debezium.Sink.LOG_PATH));
        tempTaskLogMap.put(Method.Run.REVERSE_CONNECT_SOURCE, hashtable.get(Debezium.Source.REVERSE_LOG_PATH));
        tempTaskLogMap.put(Method.Run.REVERSE_CONNECT_SINK, hashtable.get(Debezium.Sink.REVERSE_LOG_PATH));
        tempTaskLogMap.put(Method.Run.CHECK_SOURCE, hashtable.get(Check.Source.LOG_PATH));
        tempTaskLogMap.put(Method.Run.CHECK_SINK, hashtable.get(Check.Sink.LOG_PATH));
        tempTaskLogMap.put(Method.Run.CHECK, hashtable.get(Check.LOG_PATH));
        setTaskLogMap(tempTaskLogMap);
    }

    /**
     * Init run task handler hash map.
     */
    public static void initRunTaskHandlerHashMap() {
        runTaskHandlerHashMap.clear();
        Task task = new Task();
        String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        runTaskHandlerHashMap.put(Method.Run.ZOOKEEPER, (event) -> task.runZookeeper(kafkaPath));
        runTaskHandlerHashMap.put(Method.Run.KAFKA, (event) -> task.runKafka(kafkaPath));
        runTaskHandlerHashMap.put(Method.Run.REGISTRY, (event) -> task.runSchemaRegistry(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CONNECT_SOURCE, (event) -> task.runKafkaConnectSource(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CONNECT_SINK, (event) -> task.runKafkaConnectSink(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.REVERSE_CONNECT_SOURCE, (event) -> task.runReverseKafkaConnectSource(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.REVERSE_CONNECT_SINK, (event) -> task.runReverseKafkaConnectSink(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK_SINK, (event) -> task.runDataCheckSink(datacheckPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK_SOURCE, (event) -> task.runDataCheckSource(datacheckPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK, (event) -> task.runDataCheck(datacheckPath));
    }

    /**
     * Init stop task handler hash map.
     */
    public static void initStopTaskHandlerHashMap() {
        stopTaskHandlerHashMap.clear();
        Task task = new Task();
        String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        stopTaskHandlerHashMap.put(Method.Stop.ZOOKEEPER, (event) -> task.stopZookeeper(kafkaPath));
        stopTaskHandlerHashMap.put(Method.Stop.KAFKA, (event) -> task.stopKafka(kafkaPath));
        stopTaskHandlerHashMap.put(Method.Stop.REGISTRY, (event) -> task.stopKafkaSchema(confluentPath));
        stopTaskHandlerHashMap.put(Method.Stop.CONNECT_SOURCE, (event) -> task.stopKafkaConnectSource());
        stopTaskHandlerHashMap.put(Method.Stop.CONNECT_SINK, (event) -> task.stopKafkaConnectSink());
        stopTaskHandlerHashMap.put(Method.Stop.REVERSE_CONNECT_SOURCE, (event) -> task.stopReverseKafkaConnectSource());
        stopTaskHandlerHashMap.put(Method.Stop.REVERSE_CONNECT_SINK, (event) -> task.stopReverseKafkaConnectSink());
        stopTaskHandlerHashMap.put(Method.Stop.CHECK_SINK, (event) -> task.stopDataCheckSink());
        stopTaskHandlerHashMap.put(Method.Stop.CHECK_SOURCE, (event) -> task.stopDataCheckSource());
        stopTaskHandlerHashMap.put(Method.Stop.CHECK, (event) -> task.stopDataCheck());
    }

    /**
     * Start task method.
     *
     * @param methodName the method name
     * @param sleepTime  the sleep time
     * @param startSign  the start sign
     */
    public static void startTaskMethod(String methodName, int sleepTime, String startSign) {
        if (Plan.stopPlan) {
            return;
        }
        if (taskProcessMap.containsKey(methodName)) {
            String methodProcessName = taskProcessMap.get(methodName);
            long pid = Tools.getCommandPid(methodProcessName);
            List<RunningTaskThread> runningTaskThreadList = Plan.getRunningTaskThreadsList();
            String logPath = taskLogMap.get(methodName);
            RunningTaskThread runningTaskThread = new RunningTaskThread(methodName, methodProcessName, logPath);
            if (pid == -1) {
                runningTaskThread.startTask();
                if (!startSign.equals("")) {
                    long timestamp = System.currentTimeMillis();
                    while (sleepTime > 0) {
                        Tools.sleepThread(1000, "starting task");
                        sleepTime -= 1000;
                        try {
                            if (Tools.readFileStartSign(logPath, startSign, timestamp)) {
                                break;
                            }
                        } catch (PortalException e) {
                            LOGGER.error(e.toString());
                            break;
                        }
                    }
                } else {
                    Tools.sleepThread(sleepTime, "starting task");
                }
                pid = Tools.getCommandPid(methodProcessName);
                runningTaskThread.setPid(pid);
                runningTaskThreadList.add(runningTaskThread);
                Plan.setRunningTaskThreadsList(runningTaskThreadList);
            } else if (runningTaskThreadList.contains(runningTaskThread)) {
                Tools.sleepThread(sleepTime, "starting task");
                LOGGER.info(methodName + " has started.");
            } else {
                Tools.sleepThread(sleepTime, "starting task");
                LOGGER.info(methodName + " has started.");
                runningTaskThread.setPid(Tools.getCommandPid(methodProcessName));
            }
        }
    }

    /**
     * Stop task method.
     *
     * @param methodName the method name
     */
    public static void stopTaskMethod(String methodName) {
        List<RunningTaskThread> runningTaskThreadThreadList = Plan.getRunningTaskThreadsList();
        int index = -1;
        for (RunningTaskThread runningTaskThread : runningTaskThreadThreadList) {
            if (runningTaskThread.getMethodName().equals(methodName)) {
                runningTaskThread.stopTask();
                index = runningTaskThreadThreadList.indexOf(runningTaskThread);
                break;
            }
        }
        if (index != -1) {
            runningTaskThreadThreadList.remove(index);
        }
        Plan.setRunningTaskThreadsList(runningTaskThreadThreadList);
    }

    /**
     * Use chameleon replica order.
     *
     * @param chameleonVenvPath the chameleon venv path
     * @param order             the order
     * @param parametersTable   the parameters table
     */
    public void useChameleonReplicaOrder(String chameleonVenvPath, String order, Hashtable<String, String> parametersTable) {
        startChameleonReplicaOrder(chameleonVenvPath, order, parametersTable);
        checkChameleonReplicaOrder(order);
    }

    /**
     * Start chameleon replica order.
     *
     * @param chameleonVenvPath the chameleon venv path
     * @param order             the order
     * @param parametersTable   the parameters table
     */
    public void startChameleonReplicaOrder(String chameleonVenvPath, String order, Hashtable<String, String> parametersTable) {
        if (Plan.stopPlan && !order.equals("drop_replica_schema")) {
            return;
        }
        String chameleonOrder = Tools.jointChameleonOrders(parametersTable, order);
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        try {
            RuntimeExecTools.executeOrder(chameleonOrder, 2000, chameleonVenvPath, logPath, true);
        } catch (PortalException e) {
            e.setRequestInformation("Start chameleon order " + order + " failed");
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
        }

    }

    /**
     * Check chameleon replica order.
     *
     * @param order the order
     */
    public void checkChameleonReplicaOrder(String order) {
        if (Plan.stopPlan && !order.equals("drop_replica_schema")) {
            return;
        }
        String endFlag = order + " finished";
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        while (!Plan.stopPlan) {
            Tools.sleepThread(1000, "starting task");
            String processString = "chameleon " + order + " --config default_" + Plan.workspaceId;
            LOGGER.info(order + " running");
            boolean processQuit = Tools.getCommandPid(processString) == -1;
            boolean finished = Tools.lastLine(logPath).contains(endFlag);
            if (processQuit && finished) {
                LOGGER.info(order + " finished");
                break;
            } else if (processQuit) {
                String errMsg = Tools.getErrorMsg(logPath);
                PortalException e = new PortalException("Process " + processString + " exit abnormally", "checking chameleon replica order", errMsg);
                e.setRequestInformation("Run chameleon order " + order + " failed");
                e.setRepairTips("read " + logPath + " or error.log to get detailed information");
                LOGGER.error(e.toString());
                Tools.shutDownPortal(e.toString());
                break;
            }
        }
    }

    /**
     * Run zookeeper.
     *
     * @param path the path
     */
    public void runZookeeper(String path) {
        String configPath = PortalControl.toolsConfigParametersTable.get(Debezium.Zookeeper.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "zookeeper-server-start.sh");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecTools.executeStartOrder(order, 3000, "", errorPath, false, "Start zookeeper");
    }

    /**
     * Stop zookeeper.
     *
     * @param path the path
     */
    public void stopZookeeper(String path) {
        String executeFile = PathUtils.combainPath(true, path + "bin", "zookeeper-server-stop.sh");
        String order = executeFile + " " + PortalControl.toolsConfigParametersTable.get(Debezium.Zookeeper.CONFIG_PATH);
        Tools.stopPublicSoftware(Method.Run.ZOOKEEPER, executeFile, order, "zookeeper");
    }

    /**
     * Run kafka.
     *
     * @param path the path
     */
    public void runKafka(String path) {
        String configPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "kafka-server-start.sh");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecTools.executeStartOrder(order, 8000, "", errorPath, false, "Start kafka");
    }

    /**
     * Stop kafka.
     *
     * @param path the path
     */
    public void stopKafka(String path) {
        String executeFile = PathUtils.combainPath(true, path + "bin", "kafka-server-stop.sh");
        String order = executeFile + " " + PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.CONFIG_PATH);
        Tools.stopPublicSoftware(Method.Run.KAFKA, executeFile, order, "kafka");
    }

    /**
     * Run schema registry.
     *
     * @param path the path
     */
    public void runSchemaRegistry(String path) {
        String configPath = PortalControl.toolsConfigParametersTable.get(Debezium.Registry.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "schema-registry-start");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecTools.executeStartOrder(order, 3000, "", errorPath, false, "Start kafka schema registry");
    }

    /**
     * Stop kafka schema.
     *
     * @param path the path
     */
    public void stopKafkaSchema(String path) {
        String executeFile = PathUtils.combainPath(true, path + "bin", "schema-registry-stop");
        String order = executeFile + " " + PortalControl.toolsConfigParametersTable.get(Debezium.Registry.CONFIG_PATH);
        Tools.stopPublicSoftware(Method.Run.REGISTRY, executeFile, order, "kafka schema registry");
    }

    /**
     * Run kafka connect source.
     *
     * @param path the path
     */
    public void runKafkaConnectSource(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.CONNECTOR_PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        Tools.runCurl(PortalControl.portalWorkSpacePath + "curl.log", connectConfigPath);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        RuntimeExecTools.executeStartOrder(order, 3000, "", errorPath, false, "Start mysql connector source");
    }

    /**
     * Run kafka connect sink.
     *
     * @param path the path
     */
    public void runKafkaConnectSink(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.CONNECTOR_PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        RuntimeExecTools.executeStartOrder(order, 3000, "", errorPath, false, "Start mysql connector sink");
    }

    /**
     * Run reverse kafka connect source.
     *
     * @param path the path
     */
    public void runReverseKafkaConnectSource(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONNECTOR_PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        Tools.runCurl(PortalControl.portalWorkSpacePath + "curl-reverse.log", connectConfigPath);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        RuntimeExecTools.executeStartOrder(order, 5000, "", errorPath, false, "Start opengauss connector source");
    }


    /**
     * Run reverse kafka connect sink.
     *
     * @param path the path
     */
    public void runReverseKafkaConnectSink(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        RuntimeExecTools.executeStartOrder(order, 5000, "", errorPath, false, "Start opengauss connector sink");
    }

    /**
     * Stop kafka connect source.
     */
    public void stopKafkaConnectSource() {
        Tools.stopExclusiveSoftware(Method.Run.CONNECT_SOURCE, Parameter.MYSQL_CONNECTOR_SOURCE_NAME);
    }

    /**
     * Stop kafka connect sink.
     */
    public void stopKafkaConnectSink() {
        Tools.stopExclusiveSoftware(Method.Run.CONNECT_SINK, Parameter.MYSQL_CONNECTOR_SINK_NAME);
    }

    /**
     * Stop reverse kafka connect source.
     */
    public void stopReverseKafkaConnectSource() {
        Tools.stopExclusiveSoftware(Method.Run.REVERSE_CONNECT_SOURCE, Parameter.OPENGAUSS_CONNECTOR_SOURCE_NAME);
    }

    /**
     * Stop reverse kafka connect sink.
     */
    public void stopReverseKafkaConnectSink() {
        Tools.stopExclusiveSoftware(Method.Run.REVERSE_CONNECT_SINK, Parameter.OPENGAUSS_CONNECTOR_SINK_NAME);
    }

    /**
     * Run data check sink.
     *
     * @param path the path
     */
    public void runDataCheckSink(String path) {
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String extractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String order = "nohup java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + sinkConfigPath + " -jar " + path + extractName + " --sink > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 3000, datacheckPath, errorPath, false, "Start datacheck sink");
    }

    /**
     * Run data check source.
     *
     * @param path the path
     */
    public void runDataCheckSource(String path) {
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Check.Source.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String extractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String order = "nohup java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + sourceConfigPath + " -jar " + path + extractName + " --source > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 3000, datacheckPath, errorPath, false, "Start datacheck source");
    }

    /**
     * Run data check.
     *
     * @param path the path
     */
    public void runDataCheck(String path) {
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String checkConfigPath = PortalControl.toolsConfigParametersTable.get(Check.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String checkName = PortalControl.toolsConfigParametersTable.get(Check.CHECK_NAME);
        String order = "nohup java -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + checkConfigPath + " -jar " + path + checkName + " > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 1000, datacheckPath, errorPath, false, "Start datacheck");
    }

    /**
     * Stop data check.
     */
    public void stopDataCheck() {
        Tools.stopExclusiveSoftware(Method.Run.CHECK, Parameter.CHECK);
    }

    /**
     * Stop data check sink.
     */
    public void stopDataCheckSink() {
        Tools.stopExclusiveSoftware(Method.Run.CHECK_SINK, Parameter.CHECK_SINK);
    }

    /**
     * Stop data check source.
     */
    public void stopDataCheckSource() {
        Tools.stopExclusiveSoftware(Method.Run.CHECK_SOURCE, Parameter.CHECK_SOURCE);
    }

    /**
     * Check plan boolean.
     *
     * @param taskList the task list
     * @return the boolean
     */
    public static boolean checkPlan(List<String> taskList) {
        if (taskList != null) {
            if (taskList.size() == 0) {
                LOGGER.error("No task in plan.Please check the plan.");
                return false;
            } else if (taskList.size() == 1) {
                if (!ALL_TASK_LIST.contains(taskList.get(0))) {
                    LOGGER.error("The task is not valid.");
                    return false;
                } else {
                    return true;
                }
            } else {
                List<String> existingTaskList = new ArrayList<>();
                for (String task : taskList) {
                    if (!ALL_TASK_LIST.contains(task)) {
                        LOGGER.error("The task is not valid.");
                        return false;
                    }
                    if (existingTaskList.contains(task)) {
                        LOGGER.error("The task already exists.");
                        return false;
                    }
                    if (!checkDatacheckType(taskList, task)) {
                        LOGGER.error("There must be the same type of migration before datacheck.");
                        return false;
                    }
                    existingTaskList.add(task);
                }
            }
            if (!checkMigrationSequence(taskList)) {
                LOGGER.error("Please set tasks in a particular sequence.");
                return false;
            }
            addCheckTask(taskList);
        } else {
            LOGGER.error("The taskList is null.");
            return false;
        }
        return true;
    }

    private static boolean checkMigrationSequence(List<String> taskList) {
        Hashtable<String, Integer> strTable = new Hashtable<>();
        strTable.put(Command.Start.Mysql.FULL, 1);
        strTable.put(Command.Start.Mysql.FULL_CHECK, 2);
        strTable.put(Command.Start.Mysql.INCREMENTAL, 3);
        strTable.put(Command.Start.Mysql.INCREMENTAL_CHECK, 4);
        strTable.put(Command.Start.Mysql.REVERSE, 5);
        strTable.put(Command.Start.Mysql.REVERSE_CHECK, 6);
        int temp = 0;
        for (String task : taskList) {
            if (strTable.get(task) < temp) {
                return false;
            }
            temp = strTable.get(task);
        }
        return true;
    }

    private static boolean checkDatacheckType(List<String> taskList, String task) {
        if (task.contains("datacheck")) {
            int index = taskList.indexOf(task);
            if (index == 0) {
                return false;
            }
            String migrationOrder = taskList.get(taskList.indexOf(task) - 1);
            String datacheckType = task.replace(" datacheck", "");
            return migrationOrder.equals(datacheckType);
        }
        return true;
    }

    private static void addCheckTask(List<String> taskList) {
        for (String task : taskList) {
            switch (task) {
                case "start mysql full migration": {
                    Plan.checkTaskList.add(new CheckTaskMysqlFullMigration());
                    break;
                }
                case "start mysql full migration datacheck": {
                    Plan.checkTaskList.add(new CheckTaskFullDatacheck());
                    break;
                }
                case "start mysql incremental migration": {
                    Plan.checkTaskList.add(new CheckTaskIncrementalMigration());
                    break;
                }
                case "start mysql incremental migration datacheck": {
                    Plan.checkTaskList.add(new CheckTaskIncrementalDatacheck());
                    break;
                }
                case "start mysql reverse migration": {
                    Plan.checkTaskList.add(new CheckTaskReverseMigration());
                    break;
                }
                case "start mysql reverse migration datacheck": {
                    Plan.checkTaskList.add(new CheckTaskReverseDatacheck());
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }

}

