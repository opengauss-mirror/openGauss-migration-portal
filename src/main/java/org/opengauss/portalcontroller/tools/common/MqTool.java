/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.tools.common;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.task.RunningTaskThread;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;

import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;

/**
 * MqTool
 *
 * @date :2024/1/2 16:14
 * @description: MqTool
 * @version: 1.1
 */
@Slf4j
public final class MqTool implements Tool {
    private final MigrationConfluentInstanceConfig confluentInstanceConfig;

    private MqTool() {
        this.confluentInstanceConfig =
                MigrationConfluentInstanceConfig.getSystemParamAndParseEntity();
    }

    /**
     * install
     *
     * @param isDownload  isDownload
     * @return boolean
     */
    @Override
    public boolean install(boolean isDownload) {
        return true;
    }

    /**
     * init
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        if (MigrationConfluentInstanceConfig.ThirdPartySoftwareConfigType.BIND.getCode()
                .equals(confluentInstanceConfig.getThirdPartySoftwareConfigType())) {
            log.info("Start kafka success. bind from = {}", confluentInstanceConfig.getKafkaIp());
            return true;
        }
        PortalControl.initHashTable();
        Task.initRunTaskHandlerHashMap();
        PortalControl.initToolsConfigParametersTableConfluent();
        Task.setConfluentConfig(toolsConfigParametersTable, Task.getTaskProcessMap());
        // kafka change
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        Hashtable<String, String> zkConfigTable = new Hashtable<>();
        Hashtable<String, String> schemaRegistryConfig = new Hashtable<>();
        if (confluentInstanceConfig.checkNecessaryParams()) {
            // kafka
            kafkaConfigTable.put("listeners",
                    "PLAINTEXT://" + confluentInstanceConfig.getKafkaIp() + ":" + confluentInstanceConfig.getKafkaPort());
            kafkaConfigTable.put("zookeeper.connect", "localhost:" + confluentInstanceConfig.getZookeeperPort());
            // zk
            zkConfigTable.put("clientPort", confluentInstanceConfig.getZookeeperPort());
            // schema_registry change
            schemaRegistryConfig.put("listeners", "http://0.0.0.0:" + confluentInstanceConfig.getSchemaRegistryPort());
            schemaRegistryConfig.put("kafkastore.connection.url",
                    "localhost:" + confluentInstanceConfig.getZookeeperPort());
        }
        kafkaConfigTable.put("log.dirs", toolsConfigParametersTable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        kafkaConfigTable.put("delete.topic.enable", "true");
        kafkaConfigTable.put("group.initial.rebalance.delay.ms", "0");
        kafkaConfigTable.put("num.network.threads", "8");
        kafkaConfigTable.put("num.io.threads", "16");
        // zookeeper change
        zkConfigTable.put("dataDir", toolsConfigParametersTable.get(Debezium.Zookeeper.TMP_PATH));
        PropertitesUtils.changePropertiesParameters(zkConfigTable,
                toolsConfigParametersTable.get(Debezium.Zookeeper.CONFIG_PATH));
        PropertitesUtils.changePropertiesParameters(kafkaConfigTable,
                toolsConfigParametersTable.get(Debezium.Kafka.CONFIG_PATH));
        PropertitesUtils.changePropertiesParameters(schemaRegistryConfig,
                toolsConfigParametersTable.get(Debezium.Registry.CONFIG_PATH));
        return true;
    }

    /**
     * start
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean start(String workspaceId) {
        if (MigrationConfluentInstanceConfig.ThirdPartySoftwareConfigType.BIND.getCode()
                .equals(confluentInstanceConfig.getThirdPartySoftwareConfigType())) {
            log.info("Start kafka success. bind from = {}", confluentInstanceConfig.getKafkaIp());
            return true;
        }
        init(workspaceId);
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String confluentPath = hashtable.get(Debezium.Confluent.PATH);
        String configPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "config",
                "migrationConfig.properties");
        Task.startTaskMethod(Method.Name.ZOOKEEPER, 2000, "", "");
        String executeKafkaFile = PathUtils.combainPath(true, confluentPath + "bin", "kafka-topics");
        String kafkaPort = PropertitesUtils.getSinglePropertiesParameter(Parameter.Port.KAFKA, configPath);
        String kafkaOrder = executeKafkaFile + " --list --bootstrap-server " + kafkaPort;
        log.info("kafkaOrder===={}", kafkaOrder);

        Task.startTaskMethod(Method.Name.KAFKA, 10000, kafkaOrder, "Broker may not be available.");
        Task.startTaskMethod(Method.Name.REGISTRY, 3000, "", "");
        if (!checkStatus(PortalControl.workspaceId)) {
            return false;
        }
        log.info("Start kafka success.");
        return true;
    }

    /**
     * stop
     *
     * @return boolean
     */
    @Override
    public boolean stop() {
        return true;
    }

    /**
     * uninstall
     *
     * @return boolean
     */
    @Override
    public boolean uninstall() {
        PortalControl.initHashTable();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String path = hashtable.get(Debezium.Confluent.PATH);
        RunningTaskThread schemaRegistry = new RunningTaskThread(Method.Name.REGISTRY);
        String executeRegistryPath = PathUtils.combainPath(true, path + "bin", "schema-registry-stop");
        String order = executeRegistryPath + " " + hashtable.get(Debezium.Registry.CONFIG_PATH);
        schemaRegistry.stopTask(order);
        RunningTaskThread kafka = new RunningTaskThread(Method.Name.KAFKA);
        String executeKafkaPath = PathUtils.combainPath(true, path + "bin", "kafka-server-stop");
        String kafkaOrder = executeKafkaPath + " " + hashtable.get(Debezium.Kafka.CONFIG_PATH);
        kafka.stopTask(kafkaOrder);
        waitForKillKafka();
        RunningTaskThread zookeeper = new RunningTaskThread(Method.Name.ZOOKEEPER);
        String executeZookeeperPath = PathUtils.combainPath(true, path + "bin", "zookeeper-server-stop");
        String zookeeperOrder = executeZookeeperPath + " " + hashtable.get(Debezium.Zookeeper.CONFIG_PATH);
        zookeeper.stopTask(zookeeperOrder);
        return true;
    }

    /**
     * reportProgress
     *
     * @return boolean
     */
    @Override
    public boolean reportProgress(String workspaceId) {
        return true;
    }

    /**
     * check kafka process status
     *
     * @return boolean
     */
    @Override
    public boolean checkStatus(String workspaceId) {
        ArrayList<String> stringArrayList = new ArrayList<>();
        stringArrayList.add(Method.Run.ZOOKEEPER);
        stringArrayList.add(Method.Run.KAFKA);
        stringArrayList.add(Method.Run.REGISTRY);
        for (String methodName : stringArrayList) {
            if (ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(methodName)) == -1) {
                log.error("Start methond={} failed.", methodName);
                return false;
            }
        }
        return true;
    }

    /**
     * Wait for kill kafka.
     */
    private static void waitForKillKafka() {
        int waitTime = 5000;
        while (true) {
            long pid = ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.KAFKA));
            if (pid == -1) {
                break;
            } else if (waitTime < 0) {
                killKafka(pid);
                break;
            } else {
                ProcessUtils.sleepThread(1000, "stop kafka");
                waitTime -= 1000;
            }
        }
    }

    /**
     * Kill kafka.
     *
     * @param pid the pid
     */
    private static void killKafka(long pid) {
        try {
            log.warn("Force kill on process kafka.");
            RuntimeExecUtils.executeOrder("kill -9 " + pid, 1000, PortalControl.portalErrorPath);
            String tmpPath = PortalControl.portalControlPath + "tmp";
            String tmpZookeeperPath = PathUtils.combainPath(false, tmpPath, "zookeeper");
            RuntimeExecUtils.executeOrder("rm -rf " + tmpZookeeperPath, 1000, PortalControl.portalErrorPath);
            String tmpKafkaPath = PathUtils.combainPath(false, tmpPath, "kafka-logs");
            RuntimeExecUtils.executeOrder("rm -rf " + tmpKafkaPath, 1000, PortalControl.portalErrorPath);
        } catch (PortalException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * getInstance
     *
     * @return MqTool
     */
    public static MqTool getInstance() {
        return SingletonHolder.instance;
    }

    private static class SingletonHolder {
        private static final MqTool instance = new MqTool();
    }
}
