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

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import static org.opengauss.portalcontroller.constant.ToolsParamsLog.NEW_DESC_PREFIX;

/**
 * PropertitesUtils
 *
 * @date :2024/1/15 11:38
 * @description: PropertitesUtils
 * @version: 1.1
 * @since 1.1
 */
public class PropertitesUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertitesUtils.class);

    /**
     * Deletes the property configuration of the properties configuration file
     *
     * @param deleteKeys deleteKeys
     * @param path       path
     */
    public static void deletePropParameters(List<String> deleteKeys, String path) {
        BufferedReader bufReader = null;
        BufferedWriter bufWriter = null;
        try {
            bufReader = new BufferedReader(new FileReader(path));
            LinkedHashMap<String, String> propertiesMap = new LinkedHashMap<>();
            LinkedHashMap<String, String> comments = new LinkedHashMap<>();
            String line;
            String comment = "";
            while ((line = bufReader.readLine()) != null) {
                if (!line.trim().startsWith("#") && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    propertiesMap.put(parts[0].trim(), parts[1].trim());
                    comments.put(parts[0].trim(), comment);
                    comment = "";
                    continue;
                }
                if (line.trim().startsWith("#")) {
                    comment += line + System.lineSeparator();
                }
            }
            for (String deleteKey : deleteKeys) {
                propertiesMap.put(deleteKey, "");
            }
            bufWriter = new BufferedWriter(new FileWriter(path));
            for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
                if (!"".equals(entry.getValue())) {
                    if (!"".equals(comments.get(entry.getKey()))) {
                        bufWriter.write(comments.get(entry.getKey()));
                        bufWriter.flush();
                    }
                    bufWriter.write(entry.getKey() + "=" + entry.getValue() + System.lineSeparator());
                    bufWriter.flush();
                }
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "delete yml parameters",
                    e.getMessage());
            LOGGER.error("{}Failed to delete properties parameters, path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        } finally {
            try {
                if (bufReader != null) {
                    bufReader.close();
                }
                if (bufWriter != null) {
                    bufWriter.close();
                }
            } catch (IOException e) {
                LOGGER.error("close file occur exception, exp is " + e.getMessage());
            }
        }
    }

    /**
     * Change single properties parameter.
     *
     * @param key   the key
     * @param value the value
     * @param path  the path
     */
    public static void changeSinglePropertiesParameter(String key, String value, String path) {
        File file = new File(path);
        try {
            ArrayList<String> stringList = new ArrayList<>();
            if (!file.exists()) {
                LOGGER.error("{}Check properties file does not exist, path: {}", ErrorCode.FILE_NOT_FOUND, path);
                return;
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                    StandardCharsets.UTF_8));
            boolean isKeyExist = false;
            while (true) {
                String temp = bufferedReader.readLine();
                if (temp == null) {
                    break;
                }
                if (temp.length() > key.length()) {
                    String tempKey = temp.substring(0, key.length() + 1);
                    if (tempKey.equals(key + "=")) {
                        temp = key + "=" + value;
                        isKeyExist = true;
                    }
                }
                stringList.add(temp);
            }
            bufferedReader.close();
            if (!isKeyExist) {
                String temp = key + "=" + value;
                stringList.add(temp);
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),
                    StandardCharsets.UTF_8));
            for (String str : stringList) {
                bufferedWriter.write(str + System.lineSeparator());
                bufferedWriter.flush();
            }
            bufferedWriter.close();
            FileUtils.writeFile(stringList, path, false);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing single properties "
                    + "parameter", e.getMessage());
            LOGGER.error("{}Failed to change properties param, path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change properties parameters.
     *
     * @param originalTable the original table
     * @param path          the path
     */
    public static void changePropertiesParameters(Hashtable<String, String> originalTable, String path) {
        if (originalTable.isEmpty()) {
            return;
        }
        File file = new File(path);
        ArrayList<String> stringList = new ArrayList<>();
        if (!file.exists()) {
            LOGGER.error("{}Check properties file does not exist, path: {}", ErrorCode.IO_EXCEPTION, path);
            return;
        }
        try {
            Hashtable<String, String> table = new Hashtable<>();
            for (String str : originalTable.keySet()) {
                table.put(str, (String) originalTable.get(str));
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                    StandardCharsets.UTF_8));
            while (true) {
                String temp = bufferedReader.readLine();
                if (temp == null) {
                    break;
                }
                String existKey = "";
                for (String key : table.keySet()) {
                    if (temp.length() > key.length()) {
                        String tempKey = temp.substring(0, key.length() + 1);
                        if (tempKey.equals(key + "=")) {
                            temp = key + "=" + table.get(key);
                            existKey = key;
                        }
                    }
                }
                table.remove(existKey);
                stringList.add(temp);
            }
            bufferedReader.close();
            for (String key : table.keySet()) {
                if (key.startsWith(NEW_DESC_PREFIX) && !path.endsWith("migrationConfig.properties")) {
                    continue;
                }
                String descKey = NEW_DESC_PREFIX + key;
                if (table.containsKey(descKey)) {
                    stringList.add("# " + table.get(descKey));
                }
                stringList.add(key + "=" + table.get(key));
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),
                    StandardCharsets.UTF_8));
            for (String s : stringList) {
                bufferedWriter.write(s + System.lineSeparator());
                bufferedWriter.flush();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing properties parameters",
                    e.getMessage());
            LOGGER.error("{}Failed to change properties param, path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Gets single properties parameter.
     *
     * @param key  the key
     * @param path the path
     * @return the single properties parameter
     */
    public static String getSinglePropertiesParameter(String key, String path) {
        String value = "";
        Properties pps = new Properties();
        try {
            pps.load(new FileInputStream(path));
            value = pps.getProperty(key);
        } catch (FileNotFoundException e) {
            PortalException portalException = new PortalException("File not found exception", "getting single "
                    + "properties parameter " + key, e.getMessage());
            LOGGER.error("{}Check properties file does not exist, path: {}", ErrorCode.FILE_NOT_FOUND, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "getting single properties "
                    + "parameter " + key, e.getMessage());
            LOGGER.error("{}Failed to get properties param, path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        pps.clear();
        return value;
    }

    /**
     * Gets properties parameters.
     *
     * @param path the path
     * @return the properties parameters
     */
    public static Hashtable<String, String> getPropertiesParameters(String path) {
        Hashtable<String, String> table = new Hashtable<>();
        try {
            Properties pps = new Properties();
            pps.load(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
            for (Object o : pps.keySet()) {
                if (o instanceof String) {
                    table.put(o.toString(), pps.getProperty(o.toString()));
                }
            }
            pps.clear();
        } catch (FileNotFoundException e) {
            PortalException portalException = new PortalException("File not found exception", "getting properties "
                    + "parameters", e.getMessage());
            LOGGER.error("{}Check properties file does not exist, path: {}", ErrorCode.FILE_NOT_FOUND, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "getting properties parameters",
                    e.getMessage());
            LOGGER.error("{}Failed to get properties param, path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        return table;
    }
}
