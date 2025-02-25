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

package org.opengauss.portalcontroller.task;

import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.AlertLogCollectionManager;
import org.opengauss.portalcontroller.alert.AlertLogConstants;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.tools.mysql.FullDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.IncrementalMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.MysqlFullMigrationTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseDatacheckTool;
import org.opengauss.portalcontroller.tools.mysql.ReverseMigrationTool;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.opengauss.portalcontroller.PortalControl.workspaceId;

/**
 * Task
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1 The type Task.
 */
public class Task {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Task.class);
    private static final int DATA_CHECK_START_TIME = 15000;
    private static final int METHOD_START_TIME = 1000;
    public static final int PROCESS_START_TIME = 3000;
    public static final int KAFKA_START_TIME = 8000;
    public static final int REVERSE_START_TIME = 5000;
    private static HashMap<String, String> methodNameMap = new HashMap<>();
    private static HashMap<String, String> taskProcessMap = new HashMap<>();
    private static HashMap<String, String> taskLogMap = new HashMap<>();
    @Getter
    private static HashMap<String, CheckProcess> checkProcessMap = new HashMap<>();

    private static final Tool INCREMENTAL_MIGRATION_TOOL = new IncrementalMigrationTool();
    private static final Tool REVERSE_MIGRATION_TOOL = new ReverseMigrationTool();
    private static final List<StartCheckFunctional> START_CHECK_FUNCTIONAL_LIST = new LinkedList<>();

    static {
        START_CHECK_FUNCTIONAL_LIST.add(
                checkLogListener -> startTaskMethod(Method.Name.CHECK_SOURCE, DATA_CHECK_START_TIME,
                        Check.CheckLog.START_SOURCE_LOG, checkLogListener));
        START_CHECK_FUNCTIONAL_LIST.add(
                checkLogListener -> startTaskMethod(Method.Name.CHECK_SINK, DATA_CHECK_START_TIME,
                        Check.CheckLog.START_SINK_LOG, checkLogListener));
        START_CHECK_FUNCTIONAL_LIST.add(checkLogListener -> startTaskMethod(Method.Name.CHECK, DATA_CHECK_START_TIME,
                Check.CheckLog.START_CHECK_LOG, checkLogListener));
    }

    /**
     * The constant ALL_TASK_LIST.
     */
    public static final List<String> ALL_TASK_LIST = Arrays.asList(
            Plan.START_MYSQL_FULL_MIGRATION,
            Plan.START_MYSQL_FULL_MIGRATION_DATACHECK,
            Plan.START_MYSQL_INCREMENTAL_MIGRATION,
            Plan.START_MYSQL_INCREMENTAL_MIGRATION_DATACHECK,
            Plan.START_MYSQL_REVERSE_MIGRATION,
            Plan.START_MYSQL_REVERSE_MIGRATION_DATACHECK
    );

    /**
     * Gets method name map.
     *
     * @return the method name map
     */
    public static HashMap<String, String> getMethodNameMap() {
        return methodNameMap;
    }

    /**
     * Sets method name map.
     *
     * @param methodNameMap the method name map
     */
    public static void setMethodNameMap(HashMap<String, String> methodNameMap) {
        Task.methodNameMap = methodNameMap;
    }

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
     * Sets task log map.
     *
     * @param taskLogMap the task log map
     */
    public static void setTaskLogMap(HashMap<String, String> taskLogMap) {
        Task.taskLogMap = taskLogMap;
    }

    /**
     * Gets task log map.
     *
     * @return the task log map
     */
    public static HashMap<String, String> getTaskLogMap() {
        return taskLogMap;
    }

    /**
     * Init method name map.
     */
    public static void initMethodNameMap() {
        HashMap<String, String> tempMethodMap = new HashMap<>();
        tempMethodMap.put(Method.Name.ZOOKEEPER, Method.Run.ZOOKEEPER);
        tempMethodMap.put(Method.Name.KAFKA, Method.Run.KAFKA);
        tempMethodMap.put(Method.Name.REGISTRY, Method.Run.REGISTRY);
        tempMethodMap.put(Method.Name.CONNECT_SOURCE, Method.Run.CONNECT_SOURCE);
        tempMethodMap.put(Method.Name.CONNECT_SINK, Method.Run.CONNECT_SINK);
        tempMethodMap.put(Method.Name.REVERSE_CONNECT_SOURCE, Method.Run.REVERSE_CONNECT_SOURCE);
        tempMethodMap.put(Method.Name.REVERSE_CONNECT_SINK, Method.Run.REVERSE_CONNECT_SINK);
        tempMethodMap.put(Method.Name.CHECK_SOURCE, Method.Run.CHECK_SOURCE);
        tempMethodMap.put(Method.Name.CHECK_SINK, Method.Run.CHECK_SINK);
        tempMethodMap.put(Method.Name.CHECK, Method.Run.CHECK);
        setMethodNameMap(tempMethodMap);
    }

    /**
     * Init task process map.
     */
    public static void initTaskProcessMap() {
        HashMap<String, String> tempTaskProcessMap = new HashMap<>();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        tempTaskProcessMap.put(Method.Run.CONNECT_SOURCE, "ConnectStandalone "
                + hashtable.get(Debezium.Source.CONNECTOR_PATH)
                + " " + hashtable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.CONNECT_SINK, "ConnectStandalone "
                + hashtable.get(Debezium.Sink.CONNECTOR_PATH)
                + " " + hashtable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.REVERSE_CONNECT_SOURCE, "ConnectStandalone "
                + hashtable.get(Debezium.Source.REVERSE_CONNECTOR_PATH)
                + " " + hashtable.get(Debezium.Source.REVERSE_CONFIG_PATH));
        tempTaskProcessMap.put(Method.Run.REVERSE_CONNECT_SINK, "ConnectStandalone "
                + hashtable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH)
                + " " + hashtable.get(Debezium.Sink.REVERSE_CONFIG_PATH));
        String datacheckPath = hashtable.get(Check.PATH);
        String extractJarName = datacheckPath + hashtable.get(Check.EXTRACT_NAME);
        String checkSourceProcessName = String.format("spring.config.additional-location=%s -jar %s "
                + "--source > /dev/null &", hashtable.get(Check.Source.CONFIG_PATH), extractJarName);
        tempTaskProcessMap.put(Method.Run.CHECK_SOURCE, checkSourceProcessName);
        String checkSinkProcessName = String.format("spring.config.additional-location=%s -jar %s --sink > /dev/null &",
                hashtable.get(Check.Sink.CONFIG_PATH), extractJarName);
        tempTaskProcessMap.put(Method.Run.CHECK_SINK, checkSinkProcessName);
        String checkJarName = datacheckPath + hashtable.get(Check.CHECK_NAME);
        String checkProcessName = String.format("spring.config.additional-location=%s -jar %s > /dev/null &",
                hashtable.get(Check.CONFIG_PATH), checkJarName);
        tempTaskProcessMap.put(Method.Run.CHECK, checkProcessName);
        setConfluentConfig(hashtable, tempTaskProcessMap);
        setTaskProcessMap(tempTaskProcessMap);
    }

    /**
     * Set the configuration information of Confluent Kafka zk register
     *
     * @param hashtable          hashtable
     * @param tempTaskProcessMap tempTaskProcessMap
     */
    public static void setConfluentConfig(Hashtable<String, String> hashtable,
                                          HashMap<String, String> tempTaskProcessMap) {
        String confluentPath = hashtable.get(Debezium.Confluent.PATH);
        String zookeeperPath = PathUtils.combainPath(true, confluentPath + "etc", "kafka",
                "zookeeper.properties");
        tempTaskProcessMap.put(Method.Run.ZOOKEEPER, "QuorumPeerMain " + zookeeperPath);
        String kafkaPath = PathUtils.combainPath(true, confluentPath + "etc", "kafka", "server.properties");
        tempTaskProcessMap.put(Method.Run.KAFKA, "Kafka " + kafkaPath);
        String registryName = PathUtils.combainPath(true, confluentPath + "etc", "schema-registry",
                "schema-registry.properties");
        tempTaskProcessMap.put(Method.Run.REGISTRY, "SchemaRegistryMain " + registryName);
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
        String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        KafkaUtils.modifyConnectStandaloneParam(PathUtils.combainPath(true, confluentPath + "bin/"
                + "connect-standalone"));
        runTaskHandlerHashMap.put(Method.Run.ZOOKEEPER, (event) -> task.runZookeeper(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.KAFKA, (event) -> task.runKafka(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.REGISTRY, (event) -> task.runSchemaRegistry(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CONNECT_SOURCE, (event) -> task.runKafkaConnectSource(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CONNECT_SINK, (event) -> task.runKafkaConnectSink(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.REVERSE_CONNECT_SOURCE,
                (event) -> task.runReverseKafkaConnectSource(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.REVERSE_CONNECT_SINK,
                (event) -> task.runReverseKafkaConnectSink(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK_SINK, (event) -> task.runDataCheckSink(datacheckPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK_SOURCE, (event) -> task.runDataCheckSource(datacheckPath));
        runTaskHandlerHashMap.put(Method.Run.CHECK, (event) -> task.runDataCheck(datacheckPath));
    }

    /**
     * initCheckProcessMap
     */
    public static void initCheckProcessMap() {
        checkProcessMap.put(Method.Name.CONNECT_SINK, () -> INCREMENTAL_MIGRATION_TOOL.checkStatus(workspaceId));
        checkProcessMap.put(Method.Name.CONNECT_SOURCE, () -> INCREMENTAL_MIGRATION_TOOL.checkStatus(workspaceId));
        checkProcessMap.put(Method.Name.REVERSE_CONNECT_SOURCE, () -> REVERSE_MIGRATION_TOOL.checkStatus(workspaceId));
        checkProcessMap.put(Method.Name.REVERSE_CONNECT_SINK, () -> REVERSE_MIGRATION_TOOL.checkStatus(workspaceId));
    }

    /**
     * Start task method.
     *
     * @param name        the name
     * @param sleepTime   the sleep time MILLISECONDS
     * @param startSign   the start sign
     * @param logListener the LogFileListener
     */
    public static void startTaskMethod(String name, int sleepTime, String startSign, LogFileListener logListener) {
        if (Plan.stopPlan) {
            return;
        }
        String runningInformation = "starting task";
        RunningTaskThread runningTaskThread = new RunningTaskThread(name);
        String processName = runningTaskThread.getProcessName();
        List<RunningTaskThread> runningTaskThreadList = Plan.getRunningTaskThreadsList();
        long pid = runningTaskThread.getPid();
        if (pid == -1) {
            runningTaskThread.startTask();
            runTaskMethodWithSign(runningInformation, sleepTime, startSign, logListener);
            pid = ProcessUtils.getCommandPid(processName);
            runningTaskThread.setPid(pid);
            runningTaskThreadList.add(runningTaskThread);
            Plan.setRunningTaskThreadsList(runningTaskThreadList);
        } else if (runningTaskThreadList.contains(runningTaskThread)) {
            ProcessUtils.sleepThread(sleepTime, runningInformation);
            LOGGER.info("{} has started.", name);
        } else {
            ProcessUtils.sleepThread(sleepTime, runningInformation);
            LOGGER.info("{} has started.", name);
            runningTaskThread.setPid(ProcessUtils.getCommandPid(processName));
        }
    }

    /**
     * Run task method with sign.
     *
     * @param information the information
     * @param sleepTime   the sleep time
     * @param startSign   the start sign
     * @param logListener logListener
     */
    public static void runTaskMethodWithSign(String information, int sleepTime,
                                             String startSign, LogFileListener logListener) {
        if (!startSign.equals("")) {
            while (sleepTime > 0) {
                ProcessUtils.sleepThread(METHOD_START_TIME, information);
                sleepTime -= METHOD_START_TIME;
                if (LogViewUtils.checkStartSignFlag(startSign, logListener)) {
                    break;
                }
            }
        } else {
            ProcessUtils.sleepThread(sleepTime, information);
        }
    }

    /**
     * Start task method.
     *
     * @param name         the name
     * @param sleepTime    the sleep time
     * @param successOrder the success order
     * @param failSign     the fail sign
     */
    public static void startTaskMethod(String name, int sleepTime, String successOrder, String failSign) {
        RunningTaskThread runningTaskThread = new RunningTaskThread(name);
        String methodProcessName = runningTaskThread.getProcessName();
        long pid = ProcessUtils.getCommandPid(methodProcessName);
        if (pid == -1) {
            runningTaskThread.startTask();
            if (!successOrder.equals("")) {
                runTaskMethodWithOrder(name, sleepTime, successOrder, failSign);
            } else {
                ProcessUtils.sleepThread(sleepTime, "starting task");
            }
            pid = ProcessUtils.getCommandPid(methodProcessName);
            runningTaskThread.setPid(pid);
        } else {
            ProcessUtils.sleepThread(sleepTime, "starting task");
            LOGGER.info("{} has started.", name);
            runningTaskThread.setPid(ProcessUtils.getCommandPid(methodProcessName));
        }

    }

    /**
     * Run task method.
     *
     * @param name         the name
     * @param sleepTime    the sleep time
     * @param successOrder the success order
     * @param failSign     the fail sign
     */
    public static void runTaskMethodWithOrder(String name, int sleepTime, String successOrder, String failSign) {
        while (true) {
            String tmpPath = PathUtils.combainPath(true, PortalControl.portalControlPath, "tmp",
                    "test_" + Plan.workspaceId + ".txt");
            try {
                RuntimeExecUtils.executeOrder(successOrder, METHOD_START_TIME, PortalControl.portalControlPath, tmpPath,
                        true, new ArrayList<>());
                ProcessUtils.sleepThread(METHOD_START_TIME, "test " + name);
                String str = LogViewUtils.getFullLog(tmpPath);
                RuntimeExecUtils.removeFile(tmpPath, PortalControl.portalErrorPath);
                if (!str.equals("") && !str.contains(failSign)) {
                    break;
                }
            } catch (PortalException e) {
                LOGGER.error("{}{}", ErrorCode.COMMAND_EXECUTION_FAILED, e.toString());
                break;
            }
            if (sleepTime <= 0) {
                LOGGER.warn("Run " + name + " failed.");
                break;
            }
            sleepTime -= METHOD_START_TIME;
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
                index = runningTaskThreadThreadList.indexOf(runningTaskThread);
                break;
            }
        }
        if (index != -1) {
            RunningTaskThread runningTaskThread = runningTaskThreadThreadList.remove(index);
            Plan.getHasStoppedThreadList().add(runningTaskThread);
            runningTaskThread.stopTask("");
        }
    }


    /**
     * Run curl.
     *
     * @param log        the log
     * @param configFile the config file
     */
    public static void runCurl(String log, String configFile) {
        try {
            FileUtils.createFile(log, true);
        } catch (PortalException e) {
            e.setRequestInformation("Create file failed.Please ensure the file " + log + " is available to check "
                    + "whether the curl order finishes successfully.");
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, e.toString());
            PortalControl.shutDownPortal(e.toString());
            return;
        }
        String config = PropertitesUtils.getSinglePropertiesParameter("key.converter.schema.registry.url", configFile);
        config += "/config";
        if (config.contains("[")) {
            config = config.replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]");
        }
        String[] cmdParts = new String[]{"curl", "-X", "PUT", "-H", "Content-Type: application/vnd.schemaregistry"
                + ".v1+json", "--data", "{\"compatibility\": \"NONE\"}", config};
        try {
            RuntimeExecUtils.executeOrderCurrentRuntime(cmdParts, METHOD_START_TIME, log, "Run curl failed.");
        } catch (PortalException e) {
            e.setRequestInformation("Run curl failed.");
            LOGGER.error("{}{}", ErrorCode.COMMAND_EXECUTION_FAILED, e.toString());
            PortalControl.shutDownPortal(e.toString());
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
        String executeFile = PathUtils.combainPath(true, path + "bin", "zookeeper-server-start");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecUtils.executeStartOrder(order, PROCESS_START_TIME, "", errorPath, false, "Start zookeeper");
    }

    /**
     * Run kafka.
     *
     * @param path the path
     */
    public void runKafka(String path) {
        String configPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "kafka-server-start");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecUtils.executeStartOrder(order, KAFKA_START_TIME, "", errorPath, false, "Start kafka");
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
        RuntimeExecUtils.executeStartOrder(order, PROCESS_START_TIME, "", errorPath, false, "Start kafka schema registry");
    }

    /**
     * Run kafka connect source.
     *
     * @param path the path
     */
    public void runKafkaConnectSource(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.CONNECTOR_PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH);
        runCurl(PortalControl.portalWorkSpacePath + "curl.log", connectConfigPath);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String numaParams =
                PortalControl.toolsMigrationParametersTable.get(Debezium.Source.INCREMENTAL_SOURCE_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }

        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            String javaToolOptions = String.format("export JAVA_TOOL_OPTIONS=\"-Denable.alert.log.collection=true"
                            + " -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s\"",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.CONNECT_SOURCE);
            order = String.format("%s && %s", javaToolOptions, order);
        }

        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        RuntimeExecUtils.executeConnectStandaloneOrder(
                order, PROCESS_START_TIME, errorPath, "Start mysql connector source");
    }

    /**
     * Run kafka connect sink.
     *
     * @param path the path
     */
    public void runKafkaConnectSink(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.CONNECTOR_PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Sink.INCREMENTAL_SINK_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }

        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            String javaToolOptions = String.format("export JAVA_TOOL_OPTIONS=\"-Denable.alert.log.collection=true"
                            + " -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s\"",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.CONNECT_SINK);
            order = String.format("%s && %s", javaToolOptions, order);
        }

        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        RuntimeExecUtils.executeConnectStandaloneOrder(
                order, PROCESS_START_TIME, errorPath, "Start mysql connector sink");
    }

    /**
     * Run reverse kafka connect source.
     *
     * @param path the path
     */
    public void runReverseKafkaConnectSource(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONNECTOR_PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONFIG_PATH);
        runCurl(PortalControl.portalWorkSpacePath + "curl-reverse.log", connectConfigPath);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Source.REVERSE_SOURCE_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }

        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            String javaToolOptions = String.format("export JAVA_TOOL_OPTIONS=\"-Denable.alert.log.collection=true"
                            + " -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s\"",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.REVERSE_CONNECT_SOURCE);
            order = String.format("%s && %s", javaToolOptions, order);
        }

        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        RuntimeExecUtils.executeConnectStandaloneOrder(
                order, REVERSE_START_TIME, errorPath, "Start opengauss connector source");
    }


    /**
     * Run reverse kafka connect sink.
     *
     * @param path the path
     */
    public void runReverseKafkaConnectSink(String path) {
        String connectConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONNECTOR_PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        String executeFile = PathUtils.combainPath(true, path + "bin", "connect-standalone");
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Sink.REVERSE_SINK_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }

        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            String javaToolOptions = String.format("export JAVA_TOOL_OPTIONS=\"-Denable.alert.log.collection=true"
                            + " -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s\"",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.REVERSE_CONNECT_SINK);
            order = String.format("%s && %s", javaToolOptions, order);
        }

        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        RuntimeExecUtils.executeConnectStandaloneOrder(
                order, REVERSE_START_TIME, errorPath, "Start opengauss connector sink");
    }

    @Builder
    static class DataCheckRunCommand {
        String jvmParameter;
        String loaderPath;
        String otherSystemParams;
        String configPath;
        String jarPath;
        String param;

        public String getRunCommamd() {
            StringBuilder builder = new StringBuilder();
            builder.append("nohup java").append(" ")
                    .append(jvmParameter).append(" ")
                    .append("-Dloader.path=").append(loaderPath).append("lib").append(" ")
                    .append(Strings.isNotBlank(otherSystemParams) ? otherSystemParams + " " : "")
                    .append("-Dspring.config.additional-location=").append(configPath).append(" ")
                    .append("-jar").append(" ")
                    .append(jarPath).append(" ")
                    .append(param).append(Strings.isNotBlank(param) ? " " : "")
                    .append("> /dev/null &");
            return builder.toString();
        }
    }

    /**
     * Run data check sink.
     *
     * @param path the path
     */
    public void runDataCheckSink(String path) {
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_EXTRACT_SINK_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_EXTRACT_SINK_JVM);
        }
        String otherSystemParams = "";
        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            otherSystemParams = String.format(
                    "-Denable.alert.log.collection=true -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.DATA_CHECK_SINK);
        }

        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String sinkConfigPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.CONFIG_PATH);
        String extractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String order = DataCheckRunCommand.builder()
                .jvmParameter(jvmParameter)
                .loaderPath(datacheckPath)
                .configPath(sinkConfigPath)
                .otherSystemParams(otherSystemParams)
                .jarPath(path + extractName)
                .param("--sink")
                .build()
                .getRunCommamd();
        RuntimeExecUtils.executeStartOrder(order, PROCESS_START_TIME, PortalControl.portalWorkSpacePath,
                errorPath, false, "Start datacheck sink");
    }

    /**
     * Run data check source.
     *
     * @param path the path
     */
    public void runDataCheckSource(String path) {
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_EXTRACT_SOURCE_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_EXTRACT_SOURCE_JVM);
        }

        String otherSystemParams = "";
        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            otherSystemParams = String.format(
                    "-Denable.alert.log.collection=true -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.DATA_CHECK_SOURCE);
        }

        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String sourceConfigPath = PortalControl.toolsConfigParametersTable.get(Check.Source.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String extractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String order = DataCheckRunCommand.builder()
                .jvmParameter(jvmParameter)
                .loaderPath(datacheckPath)
                .configPath(sourceConfigPath)
                .otherSystemParams(otherSystemParams)
                .jarPath(path + extractName)
                .param("--source")
                .build()
                .getRunCommamd();
        RuntimeExecUtils.executeStartOrder(order, PROCESS_START_TIME, PortalControl.portalWorkSpacePath,
                errorPath, false, "Start datacheck source");
    }

    /**
     * Run data check.
     *
     * @param path the path
     */
    public void runDataCheck(String path) {
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_CHECK_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_CHECK_JVM);
        }
        String otherSystemParams = "";
        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            otherSystemParams = String.format(
                    "-Denable.alert.log.collection=true -Dkafka.bootstrapServers=%s -Dkafka.topic=%s -Dkafka.key=%s",
                    AlertLogCollectionManager.getKafkaServer(), AlertLogCollectionManager.getKafkaTopic(),
                    AlertLogConstants.AlertLogSources.DATA_CHECK_CHECK);
        }

        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String checkConfigPath = PortalControl.toolsConfigParametersTable.get(Check.CONFIG_PATH);
        String errorPath = PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH);
        String checkName = PortalControl.toolsConfigParametersTable.get(Check.CHECK_NAME);
        String order = DataCheckRunCommand.builder()
                .jvmParameter(jvmParameter)
                .loaderPath(datacheckPath)
                .configPath(checkConfigPath)
                .otherSystemParams(otherSystemParams)
                .jarPath(path + checkName)
                .param("")
                .build()
                .getRunCommamd();
        RuntimeExecUtils.executeStartOrder(order, METHOD_START_TIME, PortalControl.portalWorkSpacePath,
                errorPath, false, "Start datacheck");
    }

    /**
     * Check plan boolean.
     *
     * @param taskList the task list
     * @return the boolean
     */
    public static boolean checkPlan(List<String> taskList) {
        if (taskList != null) {
            if (taskList.isEmpty()) {
                LOGGER.error("{}No task in plan. Please check the plan.", ErrorCode.INVALID_COMMAND);
                return false;
            } else if (taskList.size() == 1) {
                if (!ALL_TASK_LIST.contains(taskList.get(0))) {
                    LOGGER.error("{}The task is not valid.", ErrorCode.INVALID_COMMAND);
                    return false;
                } else {
                    return true;
                }
            } else {
                List<String> existingTaskList = new ArrayList<>();
                for (String task : taskList) {
                    if (!ALL_TASK_LIST.contains(task)) {
                        LOGGER.error("{}The task is not valid.", ErrorCode.INVALID_COMMAND);
                        return false;
                    }
                    if (existingTaskList.contains(task)) {
                        LOGGER.error("{}The task already exists.", ErrorCode.INVALID_COMMAND);
                        return false;
                    }
                    if (!checkDatacheckType(taskList, task)) {
                        LOGGER.error("{}There must be the same type of migration before datacheck.",
                                ErrorCode.INVALID_COMMAND);
                        return false;
                    }
                    existingTaskList.add(task);
                }
            }
            if (!checkMigrationSequence(taskList)) {
                LOGGER.error("{}Please set tasks in a particular sequence.", ErrorCode.INVALID_COMMAND);
                return false;
            }
            addCheckTask(taskList);
        } else {
            LOGGER.error("{}The taskList is null.", ErrorCode.INVALID_COMMAND);
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
                case Plan.START_MYSQL_FULL_MIGRATION: {
                    Plan.toolList.add(new MysqlFullMigrationTool());
                    break;
                }
                case Plan.START_MYSQL_FULL_MIGRATION_DATACHECK: {
                    Plan.toolList.add(new FullDatacheckTool());
                    break;
                }
                case Plan.START_MYSQL_INCREMENTAL_MIGRATION: {
                    Plan.toolList.add(new IncrementalMigrationTool());
                    break;
                }
                case Plan.START_MYSQL_INCREMENTAL_MIGRATION_DATACHECK: {
                    Plan.toolList.add(new IncrementalDatacheckTool());
                    break;
                }
                case Plan.START_MYSQL_REVERSE_MIGRATION: {
                    Plan.toolList.add(new ReverseMigrationTool());
                    break;
                }
                case Plan.START_MYSQL_REVERSE_MIGRATION_DATACHECK: {
                    Plan.toolList.add(new ReverseDatacheckTool());
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }

    /**
     * Start Datacheck
     *
     * @param logFileListener DataCheckLogFileCheck
     */
    public static void startDataCheck(LogFileListener logFileListener) {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(START_CHECK_FUNCTIONAL_LIST.size());
            START_CHECK_FUNCTIONAL_LIST.parallelStream()
                                       .forEach(checkTask -> {
                                           checkTask.apply(logFileListener);
                                           countDownLatch.countDown();
                                       });
            countDownLatch.await();
        } catch (InterruptedException ex) {
            LOGGER.warn("start data check process interupted.");
        }
    }

    @FunctionalInterface
    interface StartCheckFunctional {
        void apply(LogFileListener checkLogListener);
    }

    /**
     * The interface CheckProcess.
     */
    interface CheckProcess {

        /**
         * checkStatus
         */
        void checkStatus();
    }
}

