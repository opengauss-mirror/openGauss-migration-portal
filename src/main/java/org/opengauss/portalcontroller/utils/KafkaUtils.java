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

package org.opengauss.portalcontroller.utils;

import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import static org.opengauss.portalcontroller.PortalControl.portalControlPath;
import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;

/**
 * KafkaUtils
 *
 * @date :2024/1/18 10:52
 * @description: KafkaUtils
 * @version: 1.1
 */
public class KafkaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * Change connect xml file.
     *
     * @param workspaceIdString the workspace id string
     * @param path              the path
     */
    public static void changekafkaLogParam(String workspaceIdString, String path) {
        if (Strings.isBlank(path)) {
            LOGGER.error("path is null or empty...");
            return;
        }
        try {
            StringBuilder result = new StringBuilder();
            String temp;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            while ((temp = bufferedReader.readLine()) != null) {
                if (temp.contains("/connect") && temp.contains(".log")) {
                    int start = temp.indexOf("/connect");
                    String connectLogName = temp.substring(start);
                    temp = temp.replace(connectLogName, "/connect_" + workspaceIdString + ".log");
                }
                result.append(temp).append(System.lineSeparator());
            }
            bufferedReader.close();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
            bufferedWriter.write(result.toString());
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing file parameters",
                    e.getMessage());
            LOGGER.error("{}Failed to change kafka log4j config file param: {}", ErrorCode.IO_EXCEPTION, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * add kafka connect error appender to connect-log4j.properties
     *
     * @param processName process name
     */
    public static void addKafkaConnectErrorAppender(String processName) {
        String log4jConfigPath = toolsConfigParametersTable.get(Debezium.Connector.LOG_PATTERN_PATH);
        String errorLogPath = getKafkaConnectErrorLogPath(processName);

        if (checkAppenderExists(log4jConfigPath)) {
            changeAppenderLogPath(log4jConfigPath, errorLogPath);
        } else {
            addAppender(log4jConfigPath, errorLogPath);
        }
    }

    /**
     * get kafka connect error log path
     *
     * @param processName process name
     * @return kafka connect error log path
     */
    public static String getKafkaConnectErrorLogPath(String processName) {
        String errorLogHomePath = PathUtils.combainPath(false,
                toolsConfigParametersTable.get(Debezium.LOG_PATH), "kafka-connect");
        String errorLogPath = PathUtils.combainPath(true, errorLogHomePath, processName + ".log");
        try {
            FileUtils.createFile(errorLogPath, true);
        } catch (PortalException e) {
            LOGGER.error("Failed to create file '{}'", errorLogPath, e);
        }
        return errorLogPath;
    }

    private static boolean checkAppenderExists(String log4jConfigPath) {
        Properties properties = new Properties();

        try (InputStream input = new FileInputStream(log4jConfigPath)) {
            properties.load(input);
        } catch (IOException e) {
            LOGGER.error("Failed to read the file '{}'", log4jConfigPath, e);
            return true;
        }

        String loggerKey = "log4j.logger.org.apache.kafka";
        return properties.containsKey(loggerKey);
    }

    private static void changeAppenderLogPath(String log4jConfigPath, String errorLogPath) {
        String appenderFileKey = "log4j.appender.kafkaErrorAppender.File=";
        String newAppenderFileEntry = appenderFileKey + errorLogPath;

        try {
            Path path = Paths.get(log4jConfigPath);
            List<String> lines = Files.readAllLines(path);
            boolean isLineModified = false;

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.trim().startsWith("#")) {
                    continue;
                }

                if (line.contains(appenderFileKey)) {
                    lines.set(i, newAppenderFileEntry);
                    isLineModified = true;
                    break;
                }
            }

            if (isLineModified) {
                Files.write(path, lines);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to modify the file '{}'", log4jConfigPath, e);
        }
    }

    private static void addAppender(String log4jConfigPath, String errorLogPath) {
        String appenderEntries = String.join(System.lineSeparator(),
                "log4j.logger.org.apache.kafka=ERROR, kafkaErrorAppender",
                "log4j.appender.kafkaErrorAppender=org.apache.log4j.FileAppender",
                "log4j.appender.kafkaErrorAppender.File=" + errorLogPath,
                "log4j.appender.kafkaErrorAppender.layout=org.apache.log4j.PatternLayout",
                "log4j.appender.kafkaErrorAppender.layout.ConversionPattern="
                        + "%d{yyyy-MM-dd HH:mm:ss,SSS} [%t] %p %c:(%L) - %m%n");

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(log4jConfigPath), StandardOpenOption.APPEND)) {
            writer.newLine();
            writer.write(appenderEntries);
            writer.newLine();
        } catch (IOException e) {
            LOGGER.error("Failed to write the file '{}'", log4jConfigPath, e);
        }
    }

    /**
     * Modify the installation directory of the confulent and the path of the configuration file based on the path
     * information passed in by the datakit
     */
    public static void changeConfluentDirFromSysParam() {
        MigrationConfluentInstanceConfig confluentInstanceConfig =
                MigrationConfluentInstanceConfig.getSystemParamAndParseEntity();
        if (!confluentInstanceConfig.checkNecessaryParams()) {
            LOGGER.info("no need change param");
            return;
        }
        LOGGER.info("get confluentInstanceConfig success start change param");
        Hashtable<String, String> migrationConfig = new Hashtable<>();
        migrationConfig.put(Parameter.Port.KAFKA,
                confluentInstanceConfig.getKafkaIp() + ":" + confluentInstanceConfig.getKafkaPort());
        migrationConfig.put(Parameter.Port.ZOOKEEPER,
                confluentInstanceConfig.getZkIp() + ":" + confluentInstanceConfig.getZookeeperPort());
        migrationConfig.put(Parameter.Port.SCHEMA_REGISTRY,
                confluentInstanceConfig.getSchemaRegistryIp() + ":" + confluentInstanceConfig.getSchemaRegistryPort());
        PropertitesUtils.changePropertiesParameters(migrationConfig,
                PathUtils.combainPath(true, portalControlPath + "config",
                        "migrationConfig.properties"));
        Hashtable<String, String> toolsConfig = new Hashtable<>();
        if (MigrationConfluentInstanceConfig.ThirdPartySoftwareConfigType.BIND.getCode()
                .equals(confluentInstanceConfig.getThirdPartySoftwareConfigType())) {
            LOGGER.info("no need change kafka bind from = {}", confluentInstanceConfig.getKafkaIp());
            toolsConfig.put(Debezium.Confluent.INSTALL_PATH, toolsConfigParametersTable.get(Debezium.Confluent.PATH));
            toolsConfigParametersTable.put(Debezium.Confluent.INSTALL_PATH,
                    toolsConfigParametersTable.get(Debezium.PATH));
        } else {
            toolsConfigParametersTable.put(Debezium.Confluent.PATH, PathUtils.combainPath(false,
                    confluentInstanceConfig.getInstallDir(), Debezium.Confluent.DIR_NAME));
            toolsConfigParametersTable.put(Debezium.Confluent.INSTALL_PATH, confluentInstanceConfig.getInstallDir());
            toolsConfig.put(Debezium.Confluent.PATH, toolsConfigParametersTable.get(Debezium.Confluent.PATH));
            toolsConfig.put(Debezium.Confluent.INSTALL_PATH,
                    toolsConfigParametersTable.get(Debezium.Confluent.INSTALL_PATH));
        }
        PropertitesUtils.changePropertiesParameters(toolsConfig,
                PathUtils.combainPath(true, portalControlPath + "config",
                        "toolspath.properties"));
    }

    /**
     * Prepare confluent.
     */
    public static void prepareConfluent() {
        String workDirectory = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        String cleanFileName = "clean.sh";
        RuntimeExecUtils.runShell(cleanFileName, workDirectory);
        String buildFileName = "build.sh";
        RuntimeExecUtils.runShell(buildFileName, workDirectory);
    }

    /**
     * modify kafka jvm param
     */
    public static void modifyConnectStandaloneParam(String path) {
        if (Strings.isBlank(path)) {
            LOGGER.error("path is null or empty...");
            return;
        }
        try {
            StringBuilder result = new StringBuilder();
            String temp;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            while ((temp = bufferedReader.readLine()) != null) {
                if (temp.contains("-Xms") && !temp.contains("-XX:+HeapDumpOnOutOfMemoryError")) {
                    temp = temp.substring(0, temp.lastIndexOf("\"")) + " -XX:+HeapDumpOnOutOfMemoryError "
                            + "-XX:HeapDumpPath=$base_dir/../logs/heap_source.hprof -Dfile.encoding=UTF-8\"";
                }
                result.append(temp).append(System.lineSeparator());
            }
            bufferedReader.close();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
            bufferedWriter.write(result.toString());
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing file parameters",
                    e.getMessage());
            LOGGER.error("{}Failed to change connectStandalone jvm param: {}", ErrorCode.IO_EXCEPTION, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
    }
}
