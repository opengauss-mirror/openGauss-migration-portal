/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.KafkaConstants;
import org.opengauss.domain.dto.KafkaStatusDto;
import org.opengauss.exceptions.InstallException;
import org.opengauss.exceptions.KafkaException;
import org.opengauss.migration.process.ConfluentProcess;
import org.opengauss.config.ApplicationConfig;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.PortUtils;
import org.opengauss.utils.ProcessUtils;
import org.opengauss.utils.PropertiesUtils;
import org.opengauss.utils.ThreadUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * kafka
 *
 * @since 2025/2/17
 */
@Getter
public class Kafka extends Tool {
    private static final Logger LOGGER = LogManager.getLogger(Kafka.class);
    private static final int ZOOKEEPER_START_TIME = 3000;
    private static final int KAFKA_START_TIME = 10000;
    private static final int SCHEMA_REGISTRY_START_TIME = 8000;

    private static volatile Kafka instance;

    private final String pkgDirPath;
    private final String pkgName;
    private final String installDirPath;
    private final String confluentDirPath;

    private final String kafkaPortConfigPath;
    private final String kafkaTmpDirPath;
    private final String kafkaStarterPath;
    private final String kafkaConfigPath;

    private final String zookeeperTmpDirPath;
    private final String zookeeperStarterPath;
    private final String zookeeperConfigPath;

    private final String schemaRegistryStarterPath;
    private final String schemaRegistryConfigPath;

    private final String connectStandalonePath;

    private volatile Properties kafkaPortProperties;
    private volatile ConfluentProcess zookeeperProcess;
    private volatile ConfluentProcess kafkaProcess;
    private volatile ConfluentProcess schemaRegistryProcess;

    private Kafka() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();

        this.pkgDirPath = String.format("%s/%s", applicationConfig.getPortalPkgDirPath(),
                KafkaConstants.INSTALL_PKG_DIR_NAME);
        this.pkgName = KafkaConstants.INSTALL_PKG_NAME;
        this.installDirPath = String.format("%s/%s", applicationConfig.getPortalToolsDirPath(),
                KafkaConstants.INSTALL_DIR_NAME);
        this.confluentDirPath = String.format("%s/%s", this.installDirPath, KafkaConstants.CONFLUENT_DIR_NAME);

        this.kafkaTmpDirPath = String.format("%s/%s", applicationConfig.getPortalTmpDirPath(),
                KafkaConstants.KAFKA_TMP_DIR_NAME);
        this.kafkaPortConfigPath = String.format("%s/%s", applicationConfig.getPortalDataDirPath(),
                KafkaConstants.PORT_CONFIG_NAME);
        this.kafkaStarterPath = String.format("%s/%s", confluentDirPath, KafkaConstants.KAFKA_STARTER_RELATIVE_PATH);
        this.kafkaConfigPath = String.format("%s/%s", confluentDirPath, KafkaConstants.KAFKA_CONFIG_RELATIVE_PATH);

        this.zookeeperTmpDirPath = String.format("%s/%s", applicationConfig.getPortalTmpDirPath(),
                KafkaConstants.ZOOKEEPER_TMP_DIR_NAME);
        this.zookeeperStarterPath = String.format("%s/%s", confluentDirPath,
                KafkaConstants.ZOOKEEPER_STARTER_RELATIVE_PATH);
        this.zookeeperConfigPath = String.format("%s/%s", confluentDirPath,
                KafkaConstants.ZOOKEEPER_CONFIG_RELATIVE_PATH);

        this.schemaRegistryStarterPath = String.format("%s/%s", confluentDirPath,
                KafkaConstants.SCHEMA_REGISTRY_STARTER_RELATIVE_PATH);
        this.schemaRegistryConfigPath = String.format("%s/%s", confluentDirPath,
                KafkaConstants.SCHEMA_REGISTRY_CONFIG_RELATIVE_PATH);

        this.connectStandalonePath = String.format("%s/%s", confluentDirPath,
                KafkaConstants.CONNECT_STANDALONE_RELATIVE_PATH);
    }

    /**
     * Get instance of Kafka
     *
     * @return Kafka instance
     */
    public static Kafka getInstance() {
        if (instance == null) {
            synchronized (Kafka.class) {
                if (instance == null) {
                    instance = new Kafka();
                }
            }
        }

        return instance;
    }

    @Override
    public void install() {
        if (checkInstall()) {
            LOGGER.info("Kafka is already installed");
            return;
        }

        LOGGER.info("Start to install Kafka");
        LOGGER.info("Create Kafka install directory");
        createInstallDirPath(installDirPath);

        LOGGER.info("Unzip Kafka install package");
        unzipPackage(pkgDirPath, pkgName, installDirPath);

        LOGGER.info("Check Kafka install files");
        checkKeyFileExists(connectStandalonePath);
        LOGGER.info("Install Kafka successfully");

        LOGGER.info("Init Kafka");
        initKafka();
        LOGGER.info("Start Kafka");
        start();
    }

    @Override
    public void unInstall() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return;
        }

        LOGGER.info("Stop Kafka");
        stop();

        LOGGER.info("Uninstall Kafka");
        deletePath(installDirPath);
        deletePath(kafkaTmpDirPath);
        deletePath(zookeeperTmpDirPath);
        LOGGER.info("Uninstall Kafka successfully");
    }

    @Override
    public String getToolName() {
        return KafkaConstants.TOOL_NAME;
    }

    @Override
    public boolean checkInstall() {
        try {
            checkKeyFileExists(connectStandalonePath);
            checkKeyFileExists(kafkaStarterPath);
            checkKeyFileExists(kafkaConfigPath);
            checkKeyFileExists(zookeeperStarterPath);
            checkKeyFileExists(zookeeperConfigPath);
            checkKeyFileExists(schemaRegistryStarterPath);
            checkKeyFileExists(schemaRegistryConfigPath);
        } catch (InstallException e) {
            return false;
        }
        return true;
    }

    /**
     * Start Kafka processes
     *
     * @return true if all processes start successfully, false otherwise
     */
    public boolean start() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return false;
        }

        try {
            Properties portProperties = getKafkaPortProperties();

            LOGGER.info("Wait Zookeeper start");
            ConfluentProcess confluentZookeeperProcess = getZookeeperProcess();
            if (confluentZookeeperProcess.isAlive()) {
                LOGGER.info("Zookeeper is already started");
            } else {
                confluentZookeeperProcess.start();
                String zookeeperPort = portProperties.getProperty(KafkaConstants.ZOOKEEPER_PORT_CONFIG_KEY);
                checkZookeeper(zookeeperPort);
            }

            LOGGER.info("Wait Kafka start");
            ConfluentProcess confluentKafkaProcess = getKafkaProcess();
            if (confluentKafkaProcess.isAlive()) {
                LOGGER.info("Kafka is already started");
            } else {
                confluentKafkaProcess.start();
                String kafkaPort = portProperties.getProperty(KafkaConstants.KAFKA_PORT_CONFIG_KEY);
                checkKafka(kafkaPort);
            }

            LOGGER.info("Wait Schema Registry start");
            ConfluentProcess confluentSchemaRegistryProcess = getSchemaRegistryProcess();
            if (confluentSchemaRegistryProcess.isAlive()) {
                LOGGER.info("Schema Registry is already started");
            } else {
                confluentSchemaRegistryProcess.start();
                String schemaRegistryPort = portProperties.getProperty(KafkaConstants.SCHEMA_REGISTRY_PORT_CONFIG_KEY);
                checkSchemaRegistry(schemaRegistryPort);
            }

            if (confluentZookeeperProcess.isAlive() && confluentKafkaProcess.isAlive()
                    && confluentSchemaRegistryProcess.isAlive()) {
                LOGGER.info("Start all Kafka process successfully");
                return true;
            } else {
                LOGGER.error("Start all Kafka process failed, stop remaining processes");
                stopProcess(confluentZookeeperProcess);
                stopProcess(confluentKafkaProcess);
                stopProcess(confluentSchemaRegistryProcess);
                return false;
            }
        } catch (IOException | InterruptedException e) {
            throw new KafkaException("Start all Kafka process failed", e);
        }
    }

    /**
     * Stop Kafka processes
     *
     * @return true if all processes stop successfully, false otherwise
     */
    public boolean stop() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return false;
        }

        ConfluentProcess schema = getSchemaRegistryProcess();
        stopProcess(schema);

        ConfluentProcess kafka = getKafkaProcess();
        stopProcess(kafka);

        ConfluentProcess zookeeper = getZookeeperProcess();
        stopProcess(zookeeper);

        if (zookeeper.isAlive() || kafka.isAlive() || schema.isAlive()) {
            LOGGER.error("Stop all Kafka process failed");
            return false;
        } else {
            LOGGER.info("Stop all Kafka process successfully");
            return true;
        }
    }

    /**
     * Check Kafka status
     *
     * @return true if all processes are alive, false otherwise
     */
    public boolean status() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return false;
        }

        return getKafkaProcess().isAlive() && getZookeeperProcess().isAlive() && getSchemaRegistryProcess().isAlive();
    }

    /**
     * Restart Kafka processes
     *
     * @return true if all processes restart successfully, false otherwise
     */
    public boolean restart() {
        LOGGER.info("Kafka process has exit abnormally, restarting Kafka process...");
        int tryMaxCount = 3;
        int tryIntervalMillis = 5000;
        for (int i = 0; i < tryMaxCount; i++) {
            ThreadUtils.sleep(tryIntervalMillis);
            LOGGER.info("Restarting Kafka process, attempt: {}", i + 1);

            try {
                stopProcess(getSchemaRegistryProcess());
                stopProcess(getKafkaProcess());
                stopProcess(getZookeeperProcess());

                if (start()) {
                    return true;
                }
                LOGGER.error("Failed to restart Kafka process, attempt: {}", i + 1);
            } catch (KafkaException e) {
                LOGGER.error("Failed to restart Kafka process, attempt: {}", i + 1, e);
            }
        }
        LOGGER.error("Failed to restart Kafka process after {} attempts, please check the log", tryMaxCount);
        return false;
    }

    /**
     * Clean Kafka tmp files
     */
    public void clean() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return;
        }
        stop();

        try {
            LOGGER.info("Start to clean kafka tmp files");
            if (FileUtils.checkDirExists(kafkaTmpDirPath)) {
                FileUtils.deletePath(kafkaTmpDirPath);
            }
            if (FileUtils.checkDirExists(zookeeperTmpDirPath)) {
                FileUtils.deletePath(zookeeperTmpDirPath);
            }
            LOGGER.info("Clean kafka tmp files successfully");
        } catch (IOException e) {
            throw new KafkaException("Clean Kafka failed", e);
        }
    }

    /**
     * Get Kafka status detail
     *
     * @return KafkaStatusDto
     */
    public Optional<KafkaStatusDto> getStatusDetail() {
        if (!checkInstall()) {
            LOGGER.info("Kafka is not installed");
            return Optional.empty();
        }

        KafkaStatusDto result = new KafkaStatusDto();
        result.setZookeeperRunning(getZookeeperProcess().isAlive());
        result.setKafkaRunning(getKafkaProcess().isAlive());
        result.setSchemaRegistryRunning(getSchemaRegistryProcess().isAlive());
        return Optional.of(result);
    }

    /**
     * Get Kafka process list
     *
     * @return Kafka process list
     */
    public List<ConfluentProcess> getConfluentProcessList() {
        return List.of(
                getZookeeperProcess(),
                getKafkaProcess(),
                getSchemaRegistryProcess()
        );
    }

    /**
     * Get schema registry url
     *
     * @return String schema registry url
     */
    public String getSchemaRegistryUrl() {
        Properties portProperties = getKafkaPortProperties();
        String port = portProperties.get(KafkaConstants.SCHEMA_REGISTRY_PORT_CONFIG_KEY).toString();
        return String.format("%s%s:%s", KafkaConstants.CONFLUENT_URL_PREFIX, KafkaConstants.CONFLUENT_IP, port);
    }

    /**
     * Get kafka ip:port
     *
     * @return String kafka ip:port
     */
    public String getKafkaIpPort() {
        Properties portProperties = getKafkaPortProperties();
        String port = portProperties.getProperty(KafkaConstants.KAFKA_PORT_CONFIG_KEY);
        return String.format("%s:%s", KafkaConstants.CONFLUENT_IP, port);
    }

    /**
     * Get zookeeper ip:port
     *
     * @return String zookeeper ip:port
     */
    public String getZookeeperIpPort() {
        Properties portProperties = getKafkaPortProperties();
        String port = portProperties.getProperty(KafkaConstants.ZOOKEEPER_PORT_CONFIG_KEY);
        return String.format("%s:%s", KafkaConstants.CONFLUENT_IP, port);
    }

    /**
     * Set schema registry compatibility to none
     */
    public void setSchemaCompatibilityToNone() {
        String schemaRegistryUrl = getSchemaRegistryUrl();
        String changeCurl = String.format("curl -X PUT -H \"Content-Type: application/vnd.schemaregistry.v1+json\" "
                + "--data '{\"compatibility\": \"NONE\"}' %s/config", schemaRegistryUrl);
        try {
            String curlResult = ProcessUtils.executeCommandWithResult(changeCurl);
            String jsonBody = "{\"compatibility\":\"NONE\"}";
            if (curlResult.contains(jsonBody)) {
                LOGGER.info("Schema compatibility changed to NONE");
            } else {
                LOGGER.error("Set schema compatibility to NONE failed");
                throw new KafkaException("Set schema compatibility to NONE failed");
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Set schema compatibility to NONE failed", e);
            throw new KafkaException("Set schema compatibility to NONE failed", e);
        }
    }

    /**
     * Get kafka topics
     *
     * @return List<String> kafka topics
     */
    public List<String> getKafkaTopics() {
        String kafkaIpPort = getKafkaIpPort();
        String kafkaShellPath = String.format("%s/bin/kafka-topics", confluentDirPath);
        String checkCommand = String.format("%s --bootstrap-server %s --list", kafkaShellPath, kafkaIpPort);
        try {
            String commandResult = ProcessUtils.executeCommandWithResult(checkCommand, confluentDirPath);
            String[] lines = commandResult.split("\n");
            return Arrays.stream(lines).map(String::trim).collect(Collectors.toList());
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to get kafka topics", e);
            return List.of();
        }
    }

    /**
     * Delete kafka topic
     *
     * @param topicName topic name
     */
    public void deleteKafkaTopic(String topicName) {
        String kafkaIpPort = getKafkaIpPort();
        String kafkaShellPath = String.format("%s/bin/kafka-topics", confluentDirPath);
        String checkCommand = String.format("%s --bootstrap-server %s --delete --topic %s",
                kafkaShellPath, kafkaIpPort, topicName);

        try {
            String commandResult = ProcessUtils.executeCommandWithResult(checkCommand, confluentDirPath);
            if (commandResult.contains("ERROR")) {
                LOGGER.warn("Delete kafka topic failed, topic: {}, error: {}", topicName, commandResult);
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to delete kafka topic", e);
        }
    }

    private Properties getKafkaPortProperties() {
        try {
            if (kafkaPortProperties == null) {
                kafkaPortProperties = PropertiesUtils.readProperties(kafkaPortConfigPath);
            }
            return kafkaPortProperties;
        } catch (IOException e) {
            throw new KafkaException("Read Kafka port config failed", e);
        }
    }

    private ConfluentProcess getZookeeperProcess() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();
        String zookeeperCmd = String.format("%s %s", zookeeperStarterPath, zookeeperConfigPath);
        String zookeeperCheckCmd = String.format("QuorumPeerMain %s", zookeeperConfigPath);
        String zookeeperLogPath = String.format("%s/%s", applicationConfig.getPortalLogsDirPath(), "zookeeper.log");
        if (zookeeperProcess == null) {
            zookeeperProcess = new ConfluentProcess("zookeeper", zookeeperCmd, zookeeperCheckCmd, zookeeperLogPath,
                    ZOOKEEPER_START_TIME);
        }
        return zookeeperProcess;
    }

    private ConfluentProcess getKafkaProcess() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();
        String kafkaCmd = String.format("%s %s", kafkaStarterPath, kafkaConfigPath);
        String kafkaCheckCmd = String.format("SupportedKafka %s", kafkaConfigPath);
        String kafkaLogPath = String.format("%s/%s", applicationConfig.getPortalLogsDirPath(), "kafka.log");
        if (kafkaProcess == null) {
            kafkaProcess = new ConfluentProcess("kafka", kafkaCmd, kafkaCheckCmd, kafkaLogPath, KAFKA_START_TIME);
        }
        return kafkaProcess;
    }

    private ConfluentProcess getSchemaRegistryProcess() {
        ApplicationConfig applicationConfig = ApplicationConfig.getInstance();
        String schemaRegistryCmd = String.format("%s %s", schemaRegistryStarterPath, schemaRegistryConfigPath);
        String schemaRegistryCheckCmd = String.format("SchemaRegistryMain %s", schemaRegistryConfigPath);
        String schemaRegistryLogPath = String.format("%s/%s", applicationConfig.getPortalLogsDirPath(),
                "schemaRegistry.log");
        if (schemaRegistryProcess == null) {
            schemaRegistryProcess = new ConfluentProcess("schema registry", schemaRegistryCmd, schemaRegistryCheckCmd,
                    schemaRegistryLogPath, SCHEMA_REGISTRY_START_TIME);
        }
        return schemaRegistryProcess;
    }

    private void initKafka() {
        try {
            int kafkaPort = PortUtils.getUsefulPort(9092);
            String kafkaServer = String.format("%s:%s", KafkaConstants.CONFLUENT_IP, kafkaPort);
            int zookeeperPort = PortUtils.getUsefulPort(2181);
            String zookeeperServer = String.format("%s:%s", KafkaConstants.CONFLUENT_IP, zookeeperPort);

            HashMap<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put("listeners", "PLAINTEXT://" + kafkaServer);
            kafkaConfig.put("zookeeper.connect", zookeeperServer);
            kafkaConfig.put("log.dirs", kafkaTmpDirPath);
            kafkaConfig.put("zookeeper.connection.timeout.ms", "30000");
            kafkaConfig.put("zookeeper.session.timeout.ms", "30000");
            kafkaConfig.put("delete.topic.enable", "true");
            kafkaConfig.put("group.initial.rebalance.delay.ms", "0");
            kafkaConfig.put("num.network.threads", "8");
            kafkaConfig.put("num.io.threads", "16");

            HashMap<String, String> zkConfig = new HashMap<>();
            zkConfig.put("clientPort", "" + zookeeperPort);
            zkConfig.put("dataDir", zookeeperTmpDirPath);

            int schemaRegistryPort = PortUtils.getUsefulPort(8081);
            HashMap<String, String> schemaRegistryConfig = new HashMap<>();
            schemaRegistryConfig.put("listeners", "http://0.0.0.0:" + schemaRegistryPort);
            schemaRegistryConfig.put("kafkastore.connection.url", zookeeperServer);

            PropertiesUtils.updateProperties(zookeeperConfigPath, zkConfig);
            PropertiesUtils.updateProperties(kafkaConfigPath, kafkaConfig);
            PropertiesUtils.updateProperties(schemaRegistryConfigPath, schemaRegistryConfig);

            Map<String, String> kafkaProperties = new HashMap<>();
            kafkaProperties.put(KafkaConstants.KAFKA_PORT_CONFIG_KEY, String.valueOf(kafkaPort));
            kafkaProperties.put(KafkaConstants.ZOOKEEPER_PORT_CONFIG_KEY, String.valueOf(zookeeperPort));
            kafkaProperties.put(KafkaConstants.SCHEMA_REGISTRY_PORT_CONFIG_KEY, String.valueOf(schemaRegistryPort));

            if (!FileUtils.checkFileExists(kafkaPortConfigPath)) {
                FileUtils.createFile(kafkaPortConfigPath);
            }
            PropertiesUtils.writeProperties(kafkaPortConfigPath, kafkaProperties);
        } catch (IOException e) {
            throw new KafkaException("Init Kafka config failed", e);
        }
    }

    private void checkZookeeper(String zookeeperPort) throws IOException, InterruptedException {
        Thread.sleep(ZOOKEEPER_START_TIME);

        String zookeeperShellPath = String.format("%s/bin/zookeeper-shell", confluentDirPath);
        String zookeeperServer = String.format("%s:%s", KafkaConstants.CONFLUENT_IP, zookeeperPort);
        String checkCommand = String.format("%s %s ls /", zookeeperShellPath, zookeeperServer);
        String result = ProcessUtils.executeCommandWithResult(checkCommand, confluentDirPath);
        LOGGER.debug("Zookeeper check result: {}", result);
        if (result.contains("[zookeeper]") || result.contains(", zookeeper]") || result.contains(", zookeeper,")) {
            LOGGER.info("Check zookeeper is running");
        } else {
            LOGGER.warn("Check zookeeper may not running");
        }
    }

    private void checkKafka(String kafkaPort) throws IOException, InterruptedException {
        Thread.sleep(KAFKA_START_TIME);

        String kafkaShellPath = String.format("%s/bin/kafka-topics", confluentDirPath);
        String kafkaServer = String.format("%s:%s", KafkaConstants.CONFLUENT_IP, kafkaPort);
        String checkCommand = String.format("%s --bootstrap-server %s --list", kafkaShellPath, kafkaServer);
        String checkKafkaLogPath = String.format("%s/%s", ApplicationConfig.getInstance().getPortalLogsDirPath(),
                "check_kafka.log");
        long checkSleepTime = 3000L;
        String workDirPath = ApplicationConfig.getInstance().getPortalTmpDirPath();
        ProcessUtils.executeCommand(checkCommand, workDirPath, checkKafkaLogPath, checkSleepTime);

        String checkLog = FileUtils.readFileContents(checkKafkaLogPath);
        LOGGER.debug("Kafka check result: {}", checkLog);
        if (!checkLog.isBlank() && !checkLog.contains("Broker may not be available")) {
            LOGGER.info("Check kafka is running");
        } else {
            LOGGER.warn("Check kafka may not running");
        }

        FileUtils.deletePath(checkKafkaLogPath);
    }

    private void checkSchemaRegistry(String schemaRegistryPort) throws IOException, InterruptedException {
        Thread.sleep(SCHEMA_REGISTRY_START_TIME);
        String schemaRegistryUrl = String.format("%s%s:%s", KafkaConstants.CONFLUENT_URL_PREFIX,
                KafkaConstants.CONFLUENT_IP, schemaRegistryPort);
        String checkCommand = String.format("curl -X GET %s/config", schemaRegistryUrl);
        String result = ProcessUtils.executeCommandWithResult(checkCommand, confluentDirPath);
        LOGGER.debug("Schema registry check result: {}", result);
        if (result.contains("{\"compatibilityLevel\":")) {
            LOGGER.info("Check schema registry is running");
        } else {
            LOGGER.warn("Check schema registry may not running");
        }
    }

    private void stopProcess(ConfluentProcess process) {
        if (process.isAlive()) {
            LOGGER.info("Running stop {} command", process.getProcessName());
            process.stop();
            long waitTime = 1000L;
            int checkNum = (int) (process.getStartWaitTime() / waitTime);

            for (int i = 0; i < checkNum; i++) {
                ThreadUtils.sleep(waitTime);
                if (!process.isAlive()) {
                    LOGGER.info("Stop {} successfully", process.getProcessName());
                    return;
                }
            }

            try {
                ProcessUtils.killProcessByCommandSnippet(process.getCheckCommand(), true);
            } catch (IOException | InterruptedException e) {
                LOGGER.warn("Kill {} failed, error: {}", process.getProcessName(), e.getMessage());
            }

            LOGGER.info("Stop {} successfully", process.getProcessName());
        } else {
            LOGGER.info("{} is not running", process.getProcessName());
        }
    }
}
