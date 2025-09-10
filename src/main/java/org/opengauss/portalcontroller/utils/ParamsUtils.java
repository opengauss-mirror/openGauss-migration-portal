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

import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.util.Strings;
import org.jdom2.Document;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.MigrationParameters;
import org.opengauss.portalcontroller.enums.TaskParamType;
import org.opengauss.portalcontroller.enums.ToolsConfigEnum;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.WorkspacePath;
import org.opengauss.portalcontroller.verify.Constants;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.opengauss.portalcontroller.PortalControl.portalControlPath;
import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;
import static org.opengauss.portalcontroller.constant.Check.TOOLS_BLACK_LIST_CONFIG_KEY;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.KEY_SUB_INDEX;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.NEW_PARAM_PREFIX;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.NEW_DESC_PREFIX;

/**
 * ParamsUtils
 *
 * @date :2024/1/15 16:46
 * @description: ParamsUtils
 * @version: 1.1
 * @since 1.1
 */
public class ParamsUtils {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ParamsUtils.class);

    /**
     * Configurable keys that are filtered in the blacklist cannot be configured by DataKit
     *
     * @param parmaMap parmaMap
     */
    public static void filterBlackToolsParams(Map parmaMap) {
        String blackList = PropertitesUtils.getSinglePropertiesParameter(TOOLS_BLACK_LIST_CONFIG_KEY,
                PathUtils.combainPath(true, portalControlPath + "config",
                        "migrationConfig.properties"));
        if (!Strings.isBlank(blackList)) {
            String[] blackArr = blackList.split("\\|");
            List.of(blackArr).forEach(parmaMap.keySet()::remove);
        }
    }

    /**
     * Gets available ports.
     *
     * @param tempPort the temp port
     * @param size     the size
     * @param total    the total
     * @return the available ports
     */
    public static ArrayList<Integer> getAvailablePorts(int tempPort, int size, int total) {
        ArrayList<Integer> list = new ArrayList<>();
        int availablePortNumber = 0;
        for (int i = 0; i < total; i++) {
            if (isPortAvailable("127.0.0.1", tempPort)) {
                list.add(tempPort);
                availablePortNumber++;
                LOGGER.info(String.valueOf(availablePortNumber));
                if (availablePortNumber == size) {
                    break;
                }
            }
            tempPort++;
        }
        return list;
    }

    /**
     * Is port available boolean.
     *
     * @param host the host
     * @param port the port
     * @return the boolean
     */
    public static boolean isPortAvailable(String host, int port) {
        boolean flag = true;
        try {
            InetAddress address = InetAddress.getByName(host);
            Socket socket = new Socket(address, port);
            flag = false;
            socket.close();
        } catch (UnknownHostException e) {
            PortalException portalException = new PortalException("Unknown host exception", "checking port is "
                    + "available", e.getMessage());
            portalException.setRequestInformation("Unknown host address.Cannot get available ports");
            LOGGER.error("{}Failed to check host port is available", ErrorCode.MIGRATION_ENVIRONMENT_NOT_MET, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            LOGGER.info("The port " + host + ":" + port + " is available.");
        }
        return flag;
    }

    /**
     * Gets or default.
     *
     * @param parameter    the parameter
     * @param defaultValue the default value
     * @return the or default
     */
    public static String getOrDefault(String parameter, String defaultValue) {
        String value;
        if (System.getProperty(parameter) != null) {
            value = System.getProperty(parameter);
        } else {
            value = defaultValue;
        }
        return value;
    }

    /**
     * Modify the configuration file property information of the tool properties
     *
     * @param configEnum configEnum
     * @return toolsParams
     */
    public static Map<String, String> changeToolsPropsParameters(ToolsConfigEnum configEnum) {
        Hashtable<String, String> oldParams = PropertitesUtils.getPropertiesParameters(PathUtils.combainPath(true,
                portalControlPath + "config",
                "migrationConfig.properties"));
        Map<String, String> toolsParams = toolsMigrationParametersTable.entrySet().stream()
                .filter(migrationParamEntry -> migrationParamEntry.getKey()
                        .startsWith(configEnum.getType().toString()))
                .filter(migrationParamEntry ->
                        !oldParams.get(migrationParamEntry.getKey()).equals(migrationParamEntry.getValue()))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(KEY_SUB_INDEX), Map.Entry::getValue));
        Map<String, String> newParamMap = oldParams.entrySet().stream()
                .filter(migrationParamEntry -> migrationParamEntry.getKey()
                        .startsWith(NEW_PARAM_PREFIX + configEnum.getType().toString()))
                .filter(migrationParamEntry -> migrationParamEntry.getKey()
                        .startsWith(NEW_PARAM_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(NEW_PARAM_PREFIX.length()
                                + KEY_SUB_INDEX), Map.Entry::getValue));
        Map<String, String> newDescMap = oldParams.entrySet().stream()
                .filter(migrationParamEntry -> migrationParamEntry.getKey()
                        .startsWith(NEW_DESC_PREFIX + configEnum.getType().toString()))
                .collect(Collectors.toMap(entry -> NEW_DESC_PREFIX + entry.getKey().substring(NEW_DESC_PREFIX.length()
                                + KEY_SUB_INDEX), Map.Entry::getValue));
        if (!newParamMap.isEmpty()) {
            toolsParams.putAll(newParamMap);
        }
        if (!newDescMap.isEmpty()) {
            toolsParams.putAll(newDescMap);
        }
        LOGGER.info("changeToolsPropsParameters need change toolsParams:{}", toolsParams);
        return toolsParams;
    }

    /**
     * datakit the delivered parameters are initialized into the configuration file
     */
    public static void initMigrationParamsFromProps() {
        Hashtable<String, String> migrationConfig = new Hashtable<>();
        Properties properties = System.getProperties();
        properties.keySet().forEach(key -> {
            String keyStr = String.valueOf(key);
            if (keyStr.startsWith(NEW_PARAM_PREFIX)) {
                String migrationValue = System.getProperty(keyStr);
                if (Integer.parseInt(String.valueOf(keyStr.charAt(NEW_PARAM_PREFIX.length()))) == ToolsConfigEnum
                        .PORTAL_MIGRATION.getType()) {
                    migrationConfig.put(keyStr.substring(NEW_PARAM_PREFIX.length() + KEY_SUB_INDEX),
                            migrationValue);
                } else {
                    migrationConfig.put(keyStr, migrationValue);
                }
            }
            if (keyStr.startsWith(NEW_DESC_PREFIX)) {
                migrationConfig.put(keyStr, System.getProperty(keyStr));
            }
        });
        String migrationConfigPath = PathUtils.combainPath(true, portalControlPath + "config",
                "migrationConfig.properties");
        if (!migrationConfig.isEmpty()) {
            PropertitesUtils.changePropertiesParameters(migrationConfig, migrationConfigPath);
        }
        // delete key 参数
        String portalDeleteKeys = System.getProperty(ToolsConfigEnum.PORTAL_MIGRATION.getConfigName());
        if (Strings.isBlank(portalDeleteKeys)) {
            return;
        }
        PropertitesUtils.deletePropParameters(List.of(portalDeleteKeys.split(",")), migrationConfigPath);
    }

    /**
     * Load the tool's configuration file information and write to the log
     */
    public static void loadToolsConfig() {
        String portalConfigPath = PathUtils.combainPath(true, portalControlPath + "config",
                "migrationConfig.properties");
        printPropsConfigParma(portalConfigPath, ToolsConfigEnum.PORTAL_MIGRATION);
        String chameleonConfigOldPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config",
                "chameleon", "config-example.yml");
        YmlUtils.printYmlConfigParma(chameleonConfigOldPath, ToolsConfigEnum.CHAMELEON_CONFIG);
        String checkConfigParamsPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config",
                "datacheck", "application.yml");
        YmlUtils.printYmlConfigParma(checkConfigParamsPath, ToolsConfigEnum.DATA_CHECK_APPLICATION);
        String checkConfigSinkParamsPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config",
                "datacheck", "application-sink.yml");
        YmlUtils.printYmlConfigParma(checkConfigSinkParamsPath, ToolsConfigEnum.DATA_CHECK_APPLICATION_SINK);
        String checkConfigSourceParamsPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config",
                "datacheck", "application-source.yml");
        YmlUtils.printYmlConfigParma(checkConfigSourceParamsPath, ToolsConfigEnum.DATA_CHECK_APPLICATION_SOURCE);
        String debeziumSinkConfigIncrementParametersPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config", "debezium", "mysql-sink.properties");
        printPropsConfigParma(debeziumSinkConfigIncrementParametersPath, ToolsConfigEnum.DEBEZIUM_MYSQL_SINK);
        String debeziumSourceConfigIncrementParametersPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config", "debezium", "mysql-source.properties");
        printPropsConfigParma(debeziumSourceConfigIncrementParametersPath, ToolsConfigEnum.DEBEZIUM_MYSQL_SOURCE);
        String debeziumSourceConfigReverseParametersPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config", "debezium", "opengauss-source.properties");
        printPropsConfigParma(debeziumSourceConfigReverseParametersPath, ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SOURCE);
        String debeziumSinkConfigReverseParametersPath = PathUtils.combainPath(true,
                PortalControl.portalWorkSpacePath + "config", "debezium", "opengauss-sink.properties");
        printPropsConfigParma(debeziumSinkConfigReverseParametersPath, ToolsConfigEnum.DEBEZIUM_OPENGAUSS_SINK);
    }

    private static void printPropsConfigParma(String checkConfigParamsPath, ToolsConfigEnum configEnum) {
        Map<String, String> configParameters = PropertitesUtils.getPropertiesParameters(checkConfigParamsPath);
        filterBlackToolsParams(configParameters);
        LOGGER.info("{}{}{}", configEnum.getStartFromLog(),
                JSONObject.toJSONString(configParameters),
                configEnum.getEndStrFromLog());
        if (configEnum.getType().equals(ToolsConfigEnum.PORTAL_MIGRATION.getType())) {
            LOGGER.info("portal no need change");
            return;
        }
        changePortalConfig(configParameters, configEnum);
    }

    /**
     * mapping CriteriaFile type FieldtypeFieldValue
     *
     * @param parmaMap        parmaMap
     * @param toolsConfigEnum toolsConfigEnum
     */
    public static void changePortalConfig(Map parmaMap, ToolsConfigEnum toolsConfigEnum) {
        Hashtable<String, String> portalParamsMap = new Hashtable<>();
        parmaMap.forEach((key, value) -> {
            portalParamsMap.put(toolsConfigEnum.getType() + "." + getParamValueType(value) + "." + key,
                    getParamStringValue(value));
        });
        PropertitesUtils.changePropertiesParameters(portalParamsMap, PathUtils.combainPath(true,
                portalControlPath + "config",
                "migrationConfig.properties"));
    }

    /**
     * Determine the data type based on the value passed in
     *
     * @param value value
     * @return Integer Integer
     */
    public static Integer getParamValueType(Object value) {
        if (value instanceof List) {
            return TaskParamType.TYPE_LIST.getCode();
        } else if (value instanceof Integer) {
            return TaskParamType.TYPE_NUMBER.getCode();
        } else if (value instanceof Boolean) {
            return TaskParamType.TYPE_BOOLEAN.getCode();
        } else {
            return TaskParamType.TYPE_STRING.getCode();
        }
    }

    /**
     * convert Based On TheValueAndType PassedIn
     *
     * @param value     value
     * @param paramType paramType
     * @return Object
     */
    public static Object getParamValueByType(String value, Integer paramType) {
        try {
            if (paramType.equals(TaskParamType.TYPE_NUMBER.getCode())) {
                return Integer.parseInt(String.valueOf(value));
            }
            if (paramType.equals(TaskParamType.TYPE_BOOLEAN.getCode())) {
                if (value.equalsIgnoreCase("yes")) {
                    return true;
                } else if (value.equalsIgnoreCase("no")) {
                    return false;
                } else {
                    return Boolean.parseBoolean(value);
                }
            }
            if (paramType.equals(TaskParamType.TYPE_LIST.getCode())) {
                return List.of(value.split(","));
            }
        } catch (Exception e) {
            LOGGER.error("{} parse exception : ", value, e);
        }
        return value;
    }

    /**
     * getParamStringValue to string
     *
     * @param value value
     * @return String  String
     */
    public static String getParamStringValue(Object value) {
        if (value instanceof List) {
            return String.join(",", (List<String>) value);
        }
        return value.toString();
    }

    /**
     * Change datacheck log path.
     *
     * @param logPatternFile the log pattern file
     */
    public static void changeDatacheckLogPath(String logPatternFile) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String path = hashtable.get(logPatternFile);
        String log = LogViewUtils.getFullLog(path);
        String logHome = "<Property name=\"LOG_HOME\">logs</Property>";
        String datacheck = "datacheck";
        String logs = "logs";
        for (String str : log.split(System.lineSeparator())) {
            if (str.contains(logHome)) {
                String workspacePath = WorkspacePath.getInstance(PortalControl.portalControlPath, Plan.workspaceId)
                        .getWorkspaceLogPath();
                String dataCheckLogPath = PathUtils.combainPath(true, workspacePath, datacheck);
                FileUtils.changeFile(logs, dataCheckLogPath, path);
            }
        }
    }

    /**
     * modify TheLogLevel Of DataCheck
     *
     * @param logPatternFile logPatternFile
     */
    public static void changeDatacheckLogLevel(String logPatternFile) {
        if (Strings.isBlank(toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL))) {
            LOGGER.info("global log level param is empty");
            return;
        }
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        Optional<Document> document = XmlUtils.loadXml(hashtable.get(Check.LOG_PATTERN_PATH));
        if (document.isEmpty()) {
            LOGGER.error("xml get logHome is empty");
            return;
        }
        Optional<String> name = XmlUtils.getLog4j2Properties("name", document.get());
        if (name.isEmpty()) {
            LOGGER.error("get xml name is empty");
            return;
        }
        String logHome = " <Property name=\"LOG_LEVEL\">";
        String path = hashtable.get(logPatternFile);
        String log = LogViewUtils.getFullLog(path);
        for (String str : log.split(System.lineSeparator())) {
            if (!str.contains(logHome)) {
                continue;
            }
            String newLogLevel =
                    toolsMigrationParametersTable.get(MigrationParameters.Log.GLOBAL_LOG_LEVEL).toUpperCase();
            FileUtils.changeFile(name.get(), newLogLevel, path);
        }
    }

    /**
     * Change value string.
     *
     * @param oldString the old string
     * @param hashtable the hashtable
     * @return the string
     */
    public static String changeValue(String oldString, Hashtable<String, String> hashtable) {
        if (Strings.isBlank(oldString)) {
            return oldString;
        }
        String[] split = oldString.split("\\$\\{");
        List<String> replaceStrList = new ArrayList<>();
        for (int i = 1; i < split.length; i++) {
            String splitStr = split[i];
            if (splitStr.contains("}")) {
                String variableName = splitStr.substring(0, splitStr.indexOf("}"));
                replaceStrList.add(variableName);
            }
        }
        String newString = oldString;
        for (String variable : replaceStrList) {
            String value = hashtable.get(variable);
            if (value == null) {
                continue;
            }
            newString = newString.replace("${" + variable + "}", value);
        }
        return newString;
    }


    /**
     * write map to file
     *
     * @param resultMap map
     */
    public static void writeJsonToFile(Map<String, Object> resultMap) {
        String filePath = PortalControl.portalWorkSpacePath + Constants.CHECK_RESULT_FILE;
        File file = new File(filePath);
        RandomAccessFile randomAccessFile = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.write(JSONObject.toJSONString(resultMap).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("Failed to write map to file {}: {}", filePath, e);
        } finally {
            try {
                if (randomAccessFile != null) {
                    randomAccessFile.close();
                }
            } catch (IOException e) {
                LOGGER.error("close {} failed.", filePath);
            }
        }
    }

    /**
     * write map to properties file
     *
     * @param resultMap      map
     * @param propertiesPath path
     */
    public static void writeMapToProperties(Map<String, Object> resultMap, String propertiesPath) {
        Properties properties = new Properties();
        for (String key : resultMap.keySet()) {
            properties.setProperty(key, resultMap.get(key).toString());
        }
        try (OutputStream fos = new FileOutputStream(propertiesPath)) {
            properties.store(fos, properties.toString());
        } catch (IOException e) {
            LOGGER.error("Failed to write map to properties file: {}", propertiesPath, e);
        }
    }
}
