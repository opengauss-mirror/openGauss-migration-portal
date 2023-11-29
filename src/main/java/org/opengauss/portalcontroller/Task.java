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

import org.apache.logging.log4j.util.Strings;
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
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
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
 * @since ：1 The type Task.
 */
public class Task {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Task.class);
    private static HashMap<String, String> methodNameMap = new HashMap<>();
    private static HashMap<String, String> taskProcessMap = new HashMap<>();
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
     * @param hashtable hashtable
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
        runTaskHandlerHashMap.put(Method.Run.ZOOKEEPER, (event) -> task.runZookeeper(confluentPath));
        runTaskHandlerHashMap.put(Method.Run.KAFKA, (event) -> task.runKafka(confluentPath));
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
     * Start task method.
     *
     * @param name              the name
     * @param sleepTime         the sleep time
     * @param startSign         the start sign
     * @param logListener       the LogFileListener
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
            pid = Tools.getCommandPid(processName);
            runningTaskThread.setPid(pid);
            runningTaskThreadList.add(runningTaskThread);
            Plan.setRunningTaskThreadsList(runningTaskThreadList);
        } else if (runningTaskThreadList.contains(runningTaskThread)) {
            Tools.sleepThread(sleepTime, runningInformation);
            LOGGER.info("{} has started.", name);
        } else {
            Tools.sleepThread(sleepTime, runningInformation);
            LOGGER.info("{} has started.", name);
            runningTaskThread.setPid(Tools.getCommandPid(processName));
        }
    }

    /**
     * Run task method with sign.
     *
     * @param information       the information
     * @param sleepTime         the sleep time
     * @param startSign         the start sign
     * @param logListener       logListener
     */
    public static void runTaskMethodWithSign(String information, int sleepTime,
                                             String startSign, LogFileListener logListener) {
        if (!startSign.equals("")) {
            while (sleepTime > 0) {
                Tools.sleepThread(1000, information);
                sleepTime -= 1000;
                if (LogView.checkStartSignFlag(startSign, logListener)) {
                    break;
                }
            }
        } else {
            Tools.sleepThread(sleepTime, information);
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
        long pid = Tools.getCommandPid(methodProcessName);
        if (pid == -1) {
            runningTaskThread.startTask();
            if (!successOrder.equals("")) {
                runTaskMethodWithOrder(name, sleepTime, successOrder, failSign);
            } else {
                Tools.sleepThread(sleepTime, "starting task");
            }
            pid = Tools.getCommandPid(methodProcessName);
            runningTaskThread.setPid(pid);
        } else {
            Tools.sleepThread(sleepTime, "starting task");
            LOGGER.info("{} has started.", name);
            runningTaskThread.setPid(Tools.getCommandPid(methodProcessName));
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
                RuntimeExecTools.executeOrder(successOrder, 1000, PortalControl.portalControlPath, tmpPath,
                        true, new ArrayList<>());
                Tools.sleepThread(1000, "test " + name);
                String str = LogView.getFullLog(tmpPath);
                RuntimeExecTools.removeFile(tmpPath, PortalControl.portalErrorPath);
                if (!str.equals("") && !str.contains(failSign)) {
                    break;
                }
            } catch (PortalException e) {
                LOGGER.error(e.toString());
                break;
            }
            if (sleepTime <= 0) {
                LOGGER.warn("Run " + name + " failed.");
                break;
            }
            sleepTime -= 1000;
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
                runningTaskThread.stopTask("");
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
     * @param orderList         the order list
     */
    public void useChameleonReplicaOrder(String chameleonVenvPath, String
            order, Hashtable<String, String> parametersTable, ArrayList<String> orderList) {
        startChameleonReplicaOrder(chameleonVenvPath, order, parametersTable, orderList);
        checkChameleonReplicaOrder(order);
    }

    /**
     * Start chameleon replica order.
     *
     * @param chameleonVenvPath the chameleon venv path
     * @param order             the order
     * @param parametersTable   the parameters table
     * @param orderList         the order list
     */
    public void startChameleonReplicaOrder(String chameleonVenvPath, String
            order, Hashtable<String, String> parametersTable, ArrayList<String> orderList) {
        if (Plan.stopPlan && !Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            return;
        }
        String chameleonOrder = Tools.jointChameleonOrders(parametersTable, order);
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        try {
            RuntimeExecTools.executeOrder(chameleonOrder, 2000, chameleonVenvPath, logPath, true, orderList);
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
        if (Plan.stopPlan && !Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            return;
        }
        String endFlag = order + " finished";
        String logPath = PortalControl.toolsConfigParametersTable.get(Chameleon.LOG_PATH);
        while (!Plan.stopPlan || Chameleon.Order.FINAL_ORDER_LIST.contains(order)) {
            Tools.sleepThread(1000, "starting task");
            String processString = "chameleon " + order + " --config default_" + Plan.workspaceId;
            LOGGER.info(order + " running");
            boolean processQuit = Tools.getCommandPid(processString) == -1;
            boolean finished = Tools.lastLine(logPath).contains(endFlag);
            if (processQuit && finished) {
                LOGGER.info(order + " finished");
                break;
            } else if (processQuit) {
                String errMsg = LogView.getErrorMsg(logPath);
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
        String executeFile = PathUtils.combainPath(true, path + "bin", "zookeeper-server-start");
        String order = executeFile + " -daemon " + configPath;
        RuntimeExecTools.executeStartOrder(order, 3000, "", errorPath, false, "Start zookeeper");
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
        RuntimeExecTools.executeStartOrder(order, 8000, "", errorPath, false, "Start kafka");
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
        String numaParams =
                PortalControl.toolsMigrationParametersTable.get(Debezium.Source.INCREMENTAL_SOURCE_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }
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
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Sink.INCREMENTAL_SINK_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }
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
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Source.REVERSE_SOURCE_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sourceConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }
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
        String numaParams = PortalControl.toolsMigrationParametersTable.get(Debezium.Sink.REVERSE_SINK_NUMA_PARAMS);
        String order = executeFile + " -daemon " + connectConfigPath + " " + sinkConfigPath;
        if (Strings.isNotBlank(numaParams)) {
            order = numaParams + " " + order;
        }
        RuntimeExecTools.executeStartOrder(order, 5000, "", errorPath, false, "Start opengauss connector sink");
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
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_EXTRACT_SINK_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_EXTRACT_SINK_JVM);
        }
        String order = "nohup java " + jvmParameter + " -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + sinkConfigPath + " -jar " + path + extractName + " --sink > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 3000, PortalControl.portalWorkSpacePath, errorPath, false, "Start datacheck sink");
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
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_EXTRACT_SOURCE_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_EXTRACT_SOURCE_JVM);
        }
        String order = "nohup java " + jvmParameter + " -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + sourceConfigPath + " -jar " + path + extractName + " --source > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 3000, PortalControl.portalWorkSpacePath, errorPath, false, "Start datacheck source");
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
        String jvmParameter;
        if (PortalControl.status < Status.START_INCREMENTAL_MIGRATION) {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.FULL_CHECK_JVM);
        } else {
            jvmParameter = PortalControl.toolsMigrationParametersTable.get(Check.INCREMENTAL_CHECK_JVM);
        }
        String order = "nohup java " + jvmParameter + " -Dloader.path=" + datacheckPath + "lib -Dspring.config.additional-location=" + checkConfigPath + " -jar " + path + checkName + " > /dev/null &";
        RuntimeExecTools.executeStartOrder(order, 1000, PortalControl.portalWorkSpacePath, errorPath, false, "Start datacheck");
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

    /**
     * Start Datacheck
     *
     * @param logFileListener DataCheckLogFileCheck
     */
    public static void startDataCheck(LogFileListener logFileListener) {
        startTaskMethod(Method.Name.CHECK_SOURCE, 15000, Check.CheckLog.START_SOURCE_LOG,
                logFileListener);
        startTaskMethod(Method.Name.CHECK_SINK, 15000, Check.CheckLog.START_SINK_LOG,
                logFileListener);
        startTaskMethod(Method.Name.CHECK, 15000, Check.CheckLog.START_CHECK_LOG,
                logFileListener);
    }

}

