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
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.enums.ToolsConfigEnum;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opengauss.portalcontroller.PortalControl.portalControlPath;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.KEY_SUB_INDEX;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.NEW_PARAM_PREFIX;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.VALUE_TYPE_END_INDEX;
import static org.opengauss.portalcontroller.constant.ToolsParamsLog.VALUE_TYPE_START_INDEX;

/**
 * YmlUtils
 *
 * @date :2024/1/15 15:58
 * @description: YmlUtils
 * @version: 1.1
 * @since 1.1
 */
public class YmlUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(YmlUtils.class);

    /**
     * Change single yml parameter.
     *
     * @param key   the key
     * @param value the value
     * @param path  the path
     */
    public static void changeSingleYmlParameter(String key, Object value, String path) {
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(dumperOptions);
            LinkedHashMap<String, Object> bigMap = yaml.load(fis);
            fis.close();
            String[] keys = key.split("\\.");
            String lastKey = keys[keys.length - 1];
            Map map = bigMap;
            for (int i = 0; i < keys.length - 1; ++i) {
                String s = keys[i];
                if (map.get(s) == null || !(map.get(s) instanceof Map)) {
                    map.put(s, new HashMap(4));
                }
                map = (HashMap) map.get(s);
            }
            map.put(lastKey, value);
            yaml.dump(bigMap, new FileWriter(file));
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "changing single yml parameter " + key, e.getMessage());
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Delete The Attribute Configuration Of The YML File
     *
     * @param deleteKeys deleteKeys
     * @param path       path
     */
    public static void deleteYmlParameters(List<String> deleteKeys, String path) {
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(dumperOptions);
            LinkedHashMap<String, Object> bigMap = yaml.load(fis);
            fis.close();
            for (String deleteKey : deleteKeys) {
                String[] keys = deleteKey.split("\\.");
                Map map = bigMap;
                for (int i = 0; i < keys.length; ++i) {
                    String s = keys[i];
                    if (map.get(s) == null) {
                        break;
                    }
                    if (map.get(s) instanceof Map) {
                        map = (Map) map.get(s);
                        continue;
                    }
                    map.remove(s);
                }
            }
            yaml.dump(bigMap, new FileWriter(file));
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "delete yml parameters",
                    e.getMessage());
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change yml parameters.
     *
     * @param changeParametersMap the change parameters map
     * @param path                the path
     */
    public static void changeYmlParameters(Map<String, Object> changeParametersMap, String path) {
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(dumperOptions);
            LinkedHashMap<String, Object> bigMap = yaml.load(fis);
            fis.close();
            for (String key : changeParametersMap.keySet()) {
                String[] keys = key.split("\\.");
                String lastKey = keys[keys.length - 1];
                Map map = bigMap;
                for (int i = 0; i < keys.length - 1; ++i) {
                    String s = keys[i];
                    if (map.get(s) == null || !(map.get(s) instanceof Map)) {
                        map.put(s, new HashMap<>(4));
                    }
                    map = (HashMap) map.get(s);
                }
                map.put(lastKey, changeParametersMap.get(key));
            }
            yaml.dump(bigMap, new FileWriter(file));
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing yml parameters",
                    e.getMessage());
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Gets single yml parameter.
     *
     * @param key  the key
     * @param path the path
     * @return the single yml parameter
     */
    public static String getSingleYmlParameter(String key, String path) {
        String value = "";
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(dumperOptions);
            LinkedHashMap<String, Object> bigMap = yaml.load(fis);
            fis.close();
            String[] keys = key.split("\\.");
            String lastKey = keys[keys.length - 1];
            Map map = bigMap;
            for (int i = 0; i < keys.length - 1; ++i) {
                String s = keys[i];
                if (map.get(s) == null || !(map.get(s) instanceof Map)) {
                    map.put(s, new HashMap(4));
                }
                map = (HashMap) map.get(s);
            }
            if (map.get(lastKey) instanceof String) {
                value = (String) map.get(lastKey);
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "getting single yml parameter " + key, e.getMessage());
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
        return value;
    }

    /**
     * Gets yml parameters.
     *
     * @param path the path
     * @return the yml parameters
     */
    public static HashMap<String, Object> getYmlParameters(String path) {
        HashMap<String, Object> hashMap = new HashMap<>();
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(dumperOptions);
            LinkedHashMap<String, Object> bigMap = yaml.load(fis);
            fis.close();
            HashMap<String, Object> resultHash1Map = getHashMapParameters(bigMap, "");
            for (String resultKey : resultHash1Map.keySet()) {
                if (!resultKey.equals("") && resultHash1Map.get(resultKey) != null) {
                    String newKey = resultKey.substring(1);
                    hashMap.put(newKey, resultHash1Map.get(resultKey));
                }
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "getting yml parameters",
                    e.getMessage());
            LOGGER.error("{}{}", ErrorCode.IO_EXCEPTION, portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
        return hashMap;
    }

    /**
     * Passing in the tool configuration file type and profile address to modify
     * the modified value passed in from Datakit
     *
     * @param configEnum configEnum
     * @return  toolsParams
     */
    public static Map<String, Object> getChangeToolsYmlParameters(ToolsConfigEnum configEnum) {
        Hashtable<String, String> oldParams = PropertitesUtils.getPropertiesParameters(PathUtils.combainPath(true,
                portalControlPath + "config",
                "migrationConfig.properties"));
        Map<String, Object> toolsParams = toolsMigrationParametersTable.entrySet().stream()
                .filter(migrationParamEntry ->
                        migrationParamEntry.getKey().startsWith(configEnum.getType().toString()))
                .filter(migrationParamEntry ->
                        !oldParams.get(migrationParamEntry.getKey())
                                .equals(migrationParamEntry.getValue()))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(KEY_SUB_INDEX),
                        entry -> ParamsUtils.getParamValueByType(entry.getValue(),
                                Integer.parseInt(entry.getKey().substring(VALUE_TYPE_START_INDEX,
                                        VALUE_TYPE_END_INDEX)))));
        Map<String, Object> newParamMap = oldParams.entrySet().stream()
                .filter(migrationParamEntry ->
                        migrationParamEntry.getKey().startsWith(NEW_PARAM_PREFIX))
                .filter(migrationParamEntry -> migrationParamEntry.getKey()
                        .startsWith(NEW_PARAM_PREFIX + configEnum.getType().toString()))
                .collect(Collectors.toMap(entry -> entry.getKey()
                                .substring(NEW_PARAM_PREFIX.length() + KEY_SUB_INDEX),
                        entry -> ParamsUtils.getParamValueByType(entry.getValue(),
                                Integer.parseInt(entry.getKey().substring(NEW_PARAM_PREFIX.length()
                                        + VALUE_TYPE_START_INDEX, NEW_PARAM_PREFIX.length() + VALUE_TYPE_END_INDEX)))));
        if (!newParamMap.isEmpty()) {
            toolsParams.putAll(newParamMap);
        }
        LOGGER.info("changeToolsYmlParameters need change toolsParams:{}", toolsParams);
        return toolsParams;
    }

    /**
     * printYmlConfigParma
     *
     * @param checkConfigParamsPath checkConfigParamsPath
     * @param configEnum configEnum
     */
    public static void printYmlConfigParma(String checkConfigParamsPath, ToolsConfigEnum configEnum) {
        Map<String, Object> configParams = getYmlParameters(checkConfigParamsPath);
        ParamsUtils.filterBlackToolsParams(configParams);
        LOGGER.info("{}{}{}", configEnum.getStartFromLog(),
                JSONObject.toJSONString(configParams), configEnum.getEndStrFromLog());
        ParamsUtils.changePortalConfig(configParams, configEnum);
    }

    /**
     * Gets hash map parameters.
     *
     * @param hashMap    the temp hash map
     * @param currentKey the current key
     * @return the hash map parameters
     */
    public static HashMap<String, Object> getHashMapParameters(HashMap<String, Object> hashMap, String currentKey) {
        HashMap<String, Object> resultMap = new HashMap<>();
        for (String key : hashMap.keySet()) {
            String newKey = currentKey.concat(".").concat(key);
            HashMap<String, Object> tempHashMap;
            if (hashMap.get(key) instanceof HashMap) {
                tempHashMap = (HashMap) hashMap.get(key);
                HashMap<String, Object> tempResultHashMap = getHashMapParameters(tempHashMap, newKey);
                for (String resultKey : tempResultHashMap.keySet()) {
                    resultMap.put(resultKey, tempResultHashMap.get(resultKey));
                }
            } else {
                resultMap.put(newKey, hashMap.get(key));
            }
        }
        return resultMap;
    }
}
