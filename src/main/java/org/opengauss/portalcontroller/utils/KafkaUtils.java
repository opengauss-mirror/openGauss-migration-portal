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
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.PortalControl.migrationConfigPath;
import static org.opengauss.portalcontroller.PortalControl.portalControlPath;
import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.toolsConfigPath;

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
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
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
        PropertitesUtils.changePropertiesParameters(migrationConfig, migrationConfigPath);
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
}
