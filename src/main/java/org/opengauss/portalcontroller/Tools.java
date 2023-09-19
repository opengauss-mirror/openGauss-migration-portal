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

import com.alibaba.fastjson.JSON;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Offset;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Regex;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.status.CheckColumnRule;
import org.opengauss.portalcontroller.status.CheckRule;
import org.opengauss.portalcontroller.status.RuleParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedInputStream;
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
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tools
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class Tools {
    private static final Logger LOGGER = LoggerFactory.getLogger(Tools.class);

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
            PortalException portalException = new PortalException("IO exception", "changing single yml parameter " + key, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change yml parameters.
     *
     * @param changeParametersMap the change parameters map
     * @param path                the path
     */
    public static void changeYmlParameters(HashMap<String, Object> changeParametersMap, String path) {
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
            PortalException portalException = new PortalException("IO exception", "changing yml parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
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
                LOGGER.error("No such file whose path is " + path);
                return;
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
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
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
            for (String str : stringList) {
                bufferedWriter.write(str + System.lineSeparator());
                bufferedWriter.flush();
            }
            bufferedWriter.close();
            LogView.writeFile(stringList, path, false);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing single properties parameter", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change properties parameters.
     *
     * @param originalTable the original table
     * @param path          the path
     */
    public static void changePropertiesParameters(Hashtable<String, String> originalTable, String path) {
        File file = new File(path);
        ArrayList<String> stringList = new ArrayList<>();
        if (!file.exists()) {
            LOGGER.error("No such file whose path is " + path);
            return;
        }
        try {
            Hashtable<String, String> table = new Hashtable<>();
            for (String str : originalTable.keySet()) {
                table.put(str, originalTable.get(str));
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
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
                String temp = key + "=" + table.get(key);
                stringList.add(temp);
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
            for (String s : stringList) {
                bufferedWriter.write(s + System.lineSeparator());
                bufferedWriter.flush();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing properties parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Gets process.
     *
     * @return the process
     */
    public static String getProcess() {
        StringBuilder processString = new StringBuilder();
        try {
            Process pro = Runtime.getRuntime().exec(new String[]{"sh", "-c", "ps ux"});
            BufferedInputStream in = new BufferedInputStream(pro.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String s;
            while ((s = br.readLine()) != null) {
                processString.append(s).append(System.lineSeparator());
            }
            br.close();
            in.close();
            pro.waitFor();
            pro.destroy();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "search process", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "search process", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return processString.toString();
    }

    /**
     * Gets command pid.
     *
     * @param command the command
     * @return the command pid
     */
    public static int getCommandPid(String command) {
        int pid = -1;
        String processString = getProcess();
        if (!processString.equals("")) {
            String[] processArray = processString.split(System.lineSeparator());
            for (String singleProcess : processArray) {
                if (singleProcess.trim().contains(command)) {
                    String[] strs = singleProcess.split("\\s+");
                    pid = Integer.parseInt(strs[1]);
                }
            }
        }
        return pid;
    }

    /**
     * Is pid exists boolean.
     *
     * @param pid the pid
     * @return the boolean
     */
    public static boolean isPidExists(long pid) {
        String processString = getProcess();
        if (!processString.equals("")) {
            String[] processArray = processString.split(System.lineSeparator());
            for (String singleProcess : processArray) {
                String[] strs = singleProcess.split("\\s+");
                if (Long.parseLong(strs[1]) == pid) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets running task pid.
     *
     * @param sign the sign
     * @return the running task pid
     */
    public static int getRunningTaskPid(String sign) {
        int pid = -1;
        for (RunningTaskThread runningTaskThread : Plan.getRunningTaskThreadsList()) {
            if (runningTaskThread.getMethodName().equals(sign)) {
                pid = Tools.getCommandPid(runningTaskThread.getProcessName());
            }
        }
        return pid;
    }

    /**
     * Is process exists boolean.
     *
     * @param pid the pid
     * @return the boolean
     */
    public static boolean isProcessExists(long pid) {
        boolean exist = false;
        try {
            Process pro = Runtime.getRuntime().exec(new String[]{"sh", "-c", "ps ux | grep \"" + pid + "\" | grep -v grep"});
            BufferedInputStream in = new BufferedInputStream(pro.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String process;
            while ((process = br.readLine()) != null && !process.equals("")) {
                exist = true;
            }
            br.close();
            in.close();
            pro.waitFor();
            pro.destroy();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "checking process exists", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "checking process exists", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return exist;
    }

    /**
     * Close all process.
     *
     * @param command the command
     */
    public static void closeAllProcess(String command) {
        try {
            Process pro = Runtime.getRuntime().exec(new String[]{"sh", "-c", "ps ux"});
            BufferedInputStream in = new BufferedInputStream(pro.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String s;
            while ((s = br.readLine()) != null) {
                if (s.contains(command)) {
                    String[] strs = s.split("\\s+");
                    int pid = Integer.parseInt(strs[1]);
                    try {
                        RuntimeExecTools.executeOrder("kill -9 " + pid, 20, PortalControl.portalErrorPath);
                    } catch (PortalException e) {
                        e.setRequestInformation("Close chameleon failed");
                        LOGGER.error(e.toString());
                        Tools.shutDownPortal(e.toString());
                    }

                }
            }
            br.close();
            in.close();
            pro.waitFor();
            pro.destroy();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "closing chameleon process", e.getMessage());
            portalException.setRequestInformation("Close full migration tools failed");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "closing chameleon process", e.getMessage());
            portalException.setRequestInformation("Close full migration tools failed");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Check another process exist boolean.
     *
     * @param criticalWordList the critical word list
     * @return the boolean
     */
    public static boolean checkAnotherProcessExist(ArrayList<String> criticalWordList) {
        boolean signal = false;
        int count = 0;
        try {
            Process pro = Runtime.getRuntime().exec(new String[]{"sh", "-c", "ps ux"});
            BufferedInputStream in = new BufferedInputStream(pro.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String processName;
            while ((processName = br.readLine()) != null) {
                boolean flag = true;
                for (String criticalWord : criticalWordList) {
                    if (!processName.contains(criticalWord)) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    count++;
                    if (count > 1) {
                        signal = true;
                        break;
                    }
                }
            }
            br.close();
            in.close();
            pro.waitFor();
            pro.destroy();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "checking whether another portal is running", e.getMessage());
            portalException.setRequestInformation("Checking whether another portal is running failed.Some tools cannot be closed");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "checking whether another portal is running", e.getMessage());
            portalException.setRequestInformation("Checking whether another portal is running failed.Some tools cannot be closed");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return signal;
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
            PortalException portalException = new PortalException("File not found exception", "getting single properties parameter " + key, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "getting single properties parameter " + key, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
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
            pps.load(new FileInputStream(path));
            for (Object o : pps.keySet()) {
                if (o instanceof String) {
                    table.put(o.toString(), pps.getProperty(o.toString()));
                }
            }
            pps.clear();
        } catch (FileNotFoundException e) {
            PortalException portalException = new PortalException("File not found exception", "getting properties parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "getting properties parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return table;
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
            PortalException portalException = new PortalException("IO exception", "getting single yml parameter " + key, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
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
            PortalException portalException = new PortalException("IO exception", "getting yml parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return hashMap;
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

    /**
     * Last line string.
     *
     * @param path the path
     * @return the string
     */
    public static String lastLine(String path) {
        File file = new File(path);
        StringBuilder builder = new StringBuilder();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long fileLastPointer = randomAccessFile.length() - 1;
            for (long filePointer = fileLastPointer; filePointer != -1; filePointer--) {
                randomAccessFile.seek(filePointer);
                int readByte = randomAccessFile.readByte();
                if (0xA == readByte) {
                    if (filePointer == fileLastPointer) {
                        continue;
                    }
                    break;
                }
                if (0xD == readByte) {
                    if (filePointer == fileLastPointer - 1) {
                        continue;
                    }
                    break;
                }
                builder.append((char) readByte);
            }
        } catch (FileNotFoundException e) {
            PortalException portalException = new PortalException("File not found exception", "reading last line in file " + path, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "reading last line in file " + path, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return builder.reverse().toString();
    }

    /**
     * Change full migration parameters.
     *
     * @param migrationparametersTable the migrationparameters table
     */
    public static void changeFullMigrationParameters(Hashtable<String, String> migrationparametersTable) {
        String chameleonConfigPath = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
        HashMap<String, Object> chameleonMap = new HashMap<>();
        String mysqlDatabaseHost = migrationparametersTable.get(Mysql.DATABASE_HOST);
        String mysqlDatabasePort = migrationparametersTable.get(Mysql.DATABASE_PORT);
        String opengaussDatabaseHost = migrationparametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = migrationparametersTable.get(Opengauss.DATABASE_PORT);
        if (mysqlDatabaseHost.matches(Regex.IP) && mysqlDatabasePort.matches(Regex.PORT) && opengaussDatabaseHost.matches(Regex.IP) && opengaussDatabasePort.matches(Regex.PORT)) {
            chameleonMap.put(Chameleon.Parameters.Mysql.HOST, mysqlDatabaseHost);
            chameleonMap.put(Chameleon.Parameters.Mysql.PORT, mysqlDatabasePort);
            chameleonMap.put(Chameleon.Parameters.Opengauss.HOST, opengaussDatabaseHost);
            chameleonMap.put(Chameleon.Parameters.Opengauss.PORT, opengaussDatabasePort);
            chameleonMap.put(Chameleon.Parameters.Mysql.USER, migrationparametersTable.get(Mysql.USER));
            chameleonMap.put(Chameleon.Parameters.Mysql.PASSWORD, migrationparametersTable.get(Mysql.PASSWORD));
            String mysqlDatabaseName = migrationparametersTable.get(Mysql.DATABASE_NAME);
            chameleonMap.put(Chameleon.Parameters.Mysql.NAME, mysqlDatabaseName);
            chameleonMap.put(Chameleon.Parameters.Opengauss.USER, migrationparametersTable.get(Opengauss.USER));
            chameleonMap.put(Chameleon.Parameters.Opengauss.PASSWORD, migrationparametersTable.get(Opengauss.PASSWORD));
            String opengaussDatabaseName = migrationparametersTable.get(Opengauss.DATABASE_NAME);
            chameleonMap.put(Chameleon.Parameters.Opengauss.NAME, opengaussDatabaseName);
            Tools.changeSingleYmlParameter(Chameleon.Parameters.Mysql.MAPPING, null, chameleonConfigPath);
            chameleonMap.put(Chameleon.Parameters.Mysql.MAPPING + "." + mysqlDatabaseName, migrationparametersTable.get(Opengauss.DATABASE_SCHEMA));
            Tools.changeYmlParameters(chameleonMap, chameleonConfigPath);
            Tools.writeChameleonOverrideType();
        } else {
            LOGGER.error("Invalid parameters.");
        }
    }

    /**
     * Change migration datacheck parameters.
     *
     * @param migrationparametersTable the migrationparameters table
     */
    public static void changeMigrationDatacheckParameters(Hashtable<String, String> migrationparametersTable) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String datacheckSourcePath = hashtable.get(Check.Source.CONFIG_PATH);
        String datacheckSinkPath = hashtable.get(Check.Sink.CONFIG_PATH);
        String datacheckServicePath = hashtable.get(Check.CONFIG_PATH);
        int checkPort = StartPort.CHECK + PortalControl.portId * 10;
        ArrayList<Integer> portList = Tools.getAvailablePorts(checkPort, 3, 1000);
        int sourcePort = portList.get(0);
        int sinkPort = portList.get(1);
        int servicePort = portList.get(2);
        HashMap<String, Object> datacheckSourceMap = new HashMap<>();
        String mysqlDatabaseName = migrationparametersTable.get(Mysql.DATABASE_NAME);
        String mysqlDatabaseHost = migrationparametersTable.get(Mysql.DATABASE_HOST);
        String mysqlDatabasePort = migrationparametersTable.get(Mysql.DATABASE_PORT);
        String opengaussDatabaseHost = migrationparametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = migrationparametersTable.get(Opengauss.DATABASE_PORT);
        String mysqlUserName = migrationparametersTable.get(Mysql.USER);
        String mysqlUserPassword = migrationparametersTable.get(Mysql.PASSWORD);
        String opengaussUserName = migrationparametersTable.get(Opengauss.USER);
        String opengaussUserPassword = migrationparametersTable.get(Opengauss.PASSWORD);
        String opengaussDatabaseName = migrationparametersTable.get(Opengauss.DATABASE_NAME);
        String opengaussDatabaseSchema = migrationparametersTable.get(Opengauss.DATABASE_SCHEMA);
        datacheckSourceMap.put(Check.Parameters.SCHEMA, mysqlDatabaseName);
        String mysqlDatacheckUrl = "jdbc:mysql://" + mysqlDatabaseHost + ":" + mysqlDatabasePort + "/" + mysqlDatabaseName + "?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&allowPublicKeyRetrieval=true";
        datacheckSourceMap.put(Check.Parameters.URL, mysqlDatacheckUrl);
        datacheckSourceMap.put(Check.Parameters.USER_NAME, mysqlUserName);
        datacheckSourceMap.put(Check.Parameters.PASSWORD, mysqlUserPassword);
        datacheckSourceMap.put("spring.check.server-uri", "http://127.0.0.1:" + servicePort);
        datacheckSourceMap.put("server.port", sourcePort);
        datacheckSourceMap.put("logging.config", hashtable.get(Check.Source.LOG_PATTERN_PATH));
        Tools.changeYmlParameters(datacheckSourceMap, datacheckSourcePath);
        HashMap<String, Object> datacheckSinkMap = new HashMap<>();
        datacheckSinkMap.put(Check.Parameters.SCHEMA, opengaussDatabaseSchema);
        String opengaussDatacheckUrl = "jdbc:opengauss://" + opengaussDatabaseHost + ":" + opengaussDatabasePort + "/" + opengaussDatabaseName + "?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        datacheckSinkMap.put(Check.Parameters.URL, opengaussDatacheckUrl);
        datacheckSinkMap.put(Check.Parameters.USER_NAME, opengaussUserName);
        datacheckSinkMap.put(Check.Parameters.PASSWORD, opengaussUserPassword);
        datacheckSinkMap.put("spring.check.server-uri", "http://127.0.0.1:" + servicePort);
        datacheckSinkMap.put("server.port", sinkPort);
        datacheckSinkMap.put("logging.config", hashtable.get(Check.Sink.LOG_PATTERN_PATH));
        Tools.changeYmlParameters(datacheckSinkMap, datacheckSinkPath);
        HashMap<String, Object> datacheckServiceMap = new HashMap<>();
        datacheckServiceMap.put("data.check.source-uri", "http://127.0.0.1:" + sourcePort);
        datacheckServiceMap.put("data.check.sink-uri", "http://127.0.0.1:" + sinkPort);
        datacheckServiceMap.put("server.port", servicePort);
        datacheckServiceMap.put("data.check.data-path", hashtable.get(Check.Result.FULL));
        datacheckServiceMap.put("logging.config", hashtable.get(Check.LOG_PATTERN_PATH));
        Tools.changeYmlParameters(datacheckServiceMap, datacheckServicePath);
    }

    /**
     * Change incremental migration parameters.
     *
     * @param migrationparametersTable the migrationparameters table
     */
    public static void changeIncrementalMigrationParameters(Hashtable<String, String> migrationparametersTable) {
        String mysqlDatabaseName = migrationparametersTable.get(Mysql.DATABASE_NAME);
        String mysqlDatabaseHost = migrationparametersTable.get(Mysql.DATABASE_HOST);
        String mysqlDatabasePort = migrationparametersTable.get(Mysql.DATABASE_PORT);
        String opengaussDatabaseHost = migrationparametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = migrationparametersTable.get(Opengauss.DATABASE_PORT);
        String mysqlUserName = migrationparametersTable.get(Mysql.USER);
        String mysqlUserPassword = migrationparametersTable.get(Mysql.PASSWORD);
        String opengaussUserName = migrationparametersTable.get(Opengauss.USER);
        String opengaussUserPassword = migrationparametersTable.get(Opengauss.PASSWORD);
        String opengaussDatabaseName = migrationparametersTable.get(Opengauss.DATABASE_NAME);
        String openGaussSchemaName = migrationparametersTable.get(Opengauss.DATABASE_SCHEMA);
        Hashtable<String, String> debeziumMysqlTable = new Hashtable<>();
        String sourcePath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH);
        debeziumMysqlTable.put(Debezium.Source.HOST, mysqlDatabaseHost);
        debeziumMysqlTable.put(Debezium.Source.PORT, mysqlDatabasePort);
        debeziumMysqlTable.put(Debezium.Source.USER, mysqlUserName);
        debeziumMysqlTable.put(Debezium.Source.PASSWORD, mysqlUserPassword);
        debeziumMysqlTable.put(Debezium.Source.WHITELIST, mysqlDatabaseName);
        if (PortalControl.toolsMigrationParametersTable.containsKey(Offset.FILE)) {
            debeziumMysqlTable.put(Offset.FILE, PortalControl.toolsMigrationParametersTable.get(Offset.FILE));
        }
        if (PortalControl.toolsMigrationParametersTable.containsKey(Offset.POSITION)) {
            debeziumMysqlTable.put(Offset.POSITION, PortalControl.toolsMigrationParametersTable.get(Offset.POSITION));
        }
        if (PortalControl.toolsMigrationParametersTable.containsKey(Offset.GTID)) {
            debeziumMysqlTable.put(Offset.GTID, PortalControl.toolsMigrationParametersTable.get(Offset.GTID));
        }
        Tools.changePropertiesParameters(debeziumMysqlTable, sourcePath);
        String confluentMysqlSinkPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH);
        Hashtable<String, String> debeziumMysqlSinkTable = new Hashtable<>();
        debeziumMysqlSinkTable.put(Debezium.Sink.SCHEMA_MAPPING, mysqlDatabaseName + ":" + openGaussSchemaName);
        debeziumMysqlSinkTable.put(Debezium.Sink.Opengauss.USER, opengaussUserName);
        debeziumMysqlSinkTable.put(Debezium.Sink.Opengauss.PASSWORD, opengaussUserPassword);
        String opengaussDebeziumUrl = "jdbc:opengauss://" + opengaussDatabaseHost + ":" + opengaussDatabasePort + "/" + opengaussDatabaseName + "?loggerLevel=OFF";
        debeziumMysqlSinkTable.put(Debezium.Sink.Opengauss.URL, opengaussDebeziumUrl);
        Tools.changePropertiesParameters(debeziumMysqlSinkTable, confluentMysqlSinkPath);
    }

    /**
     * Change reverse migration parameters.
     *
     * @param migrationparametersTable the migrationparameters table
     */
    public static void changeReverseMigrationParameters(Hashtable<String, String> migrationparametersTable) {
        Hashtable<String, String> debeziumOpenGaussTable = new Hashtable<>();
        String sourcePath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONFIG_PATH);
        debeziumOpenGaussTable.put(Debezium.Source.HOST, migrationparametersTable.get(Opengauss.DATABASE_HOST));
        debeziumOpenGaussTable.put(Debezium.Source.PORT, migrationparametersTable.get(Opengauss.DATABASE_PORT));
        debeziumOpenGaussTable.put(Debezium.Source.USER, migrationparametersTable.get(Opengauss.USER));
        debeziumOpenGaussTable.put(Debezium.Source.PASSWORD, migrationparametersTable.get(Opengauss.PASSWORD));
        debeziumOpenGaussTable.put(Debezium.Source.NAME, migrationparametersTable.get(Opengauss.DATABASE_NAME));
        Tools.changePropertiesParameters(debeziumOpenGaussTable, sourcePath);
        Hashtable<String, String> debeziumOpenGaussSinkTable = new Hashtable<>();
        String sinkPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        debeziumOpenGaussSinkTable.put(Debezium.Sink.Mysql.USER, migrationparametersTable.get(Mysql.USER));
        debeziumOpenGaussSinkTable.put(Debezium.Sink.Mysql.PASSWORD, migrationparametersTable.get(Mysql.PASSWORD));
        debeziumOpenGaussSinkTable.put(Debezium.Sink.Mysql.NAME, migrationparametersTable.get(Mysql.DATABASE_NAME));
        debeziumOpenGaussSinkTable.put(Debezium.Sink.Mysql.PORT, migrationparametersTable.get(Mysql.DATABASE_PORT));
        debeziumOpenGaussSinkTable.put(Debezium.Sink.Mysql.URL, migrationparametersTable.get(Mysql.DATABASE_HOST));
        String mysqlDatabaseName = migrationparametersTable.get(Mysql.DATABASE_NAME);
        String openGaussSchema = migrationparametersTable.get(Opengauss.DATABASE_SCHEMA);
        debeziumOpenGaussSinkTable.put(Debezium.Sink.SCHEMA_MAPPING, openGaussSchema + ":" + mysqlDatabaseName);
        Tools.changePropertiesParameters(debeziumOpenGaussSinkTable, sinkPath);
    }

    /**
     * Find offset.
     *
     * @throws PortalException the portal exception
     */
    public static void findOffset() throws PortalException {
        String offsetPath = PortalControl.toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH);
        String sql = "select t_binlog_name,i_binlog_position,t_gtid_set from sch_chameleon.t_replica_batch;";
        try (
                Connection mysqlConnection = JdbcTools.getMysqlConnection();
                ResultSet rs = JdbcTools.getPgConnection().execSQLQuery(sql)
        ) {
            String uuid = JdbcTools.getCurrentUuid(mysqlConnection);
            LOGGER.info("Current uuid: {}", uuid);
            if (rs.next()) {
                String tBinlogName = rs.getString("t_binlog_name");
                String iBinlogPosition = rs.getString("i_binlog_position");
                String tGtidSet = rs.getString("t_gtid_set");
                String offsetGtidSet = changeGtidSet(tGtidSet, uuid);
                Hashtable<String, String> offsetHashtable = new Hashtable<>();
                offsetHashtable.put(Offset.FILE, tBinlogName);
                offsetHashtable.put(Offset.POSITION, iBinlogPosition);
                offsetHashtable.put(Offset.GTID, offsetGtidSet);
                Tools.changePropertiesParameters(offsetHashtable, offsetPath);
            }
        } catch (SQLException e) {
            throw new PortalException("SQL exception", "find offset", e.getMessage());
        }
    }

    /**
     * Change gtid set string.
     *
     * @param oldGtidSet the old gtid set
     * @param mysqlUuid  the mysql uuid
     * @return the string
     */
    public static String changeGtidSet(String oldGtidSet, String mysqlUuid) {
        StringBuilder newGtidSet = new StringBuilder();
        String[] gtidSetParts = oldGtidSet.replaceAll(System.lineSeparator(), "").split(",");
        for (String tGtidSet : gtidSetParts) {
            int uuidIndex = tGtidSet.lastIndexOf(":");
            String uuid = tGtidSet.substring(0, uuidIndex);
            if (uuid.equals(mysqlUuid) && (tGtidSet.contains("-"))) {
                int offsetIndex = tGtidSet.lastIndexOf("-") + 1;
                int offset = Integer.parseInt(tGtidSet.substring(offsetIndex));
                LOGGER.info("Offset: {}", offset);
                offset--;
                tGtidSet = tGtidSet.substring(0, offsetIndex) + offset;
            }
            newGtidSet.append(tGtidSet).append(",");
        }
        newGtidSet = new StringBuilder(newGtidSet.substring(0, newGtidSet.length() - 1));
        return newGtidSet.toString();
    }

    /**
     * Generate plan history.
     *
     * @param taskList the task list
     */
    public static void generatePlanHistory(List<String> taskList) {
        String planHistoryFilePath = PathUtils.combainPath(true, PortalControl.portalControlPath + "logs", "planHistory.log");
        File file = new File(planHistoryFilePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss");
            ArrayList<String> planInforamtionPatrs = new ArrayList<>();
            planInforamtionPatrs.add(dateFormat.format(date));
            planInforamtionPatrs.add("Current plan: ");
            planInforamtionPatrs.addAll(taskList);
            for (String str : planInforamtionPatrs) {
                LOGGER.info(str);
            }
            StringBuilder planInformation = new StringBuilder();
            for (String str : planInforamtionPatrs) {
                planInformation.append(str).append(System.lineSeparator());
            }
            LogView.writeFile(planInformation.toString(), planHistoryFilePath, true);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "generating plan history", e.getMessage());
            portalException.setRequestInformation("Generating plan history failed");
            LOGGER.error(portalException.toString());
        }
    }

    /**
     * Read input order.
     */
    public static void readInputOrder() {
        String path = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        String fullLog = LogView.getFullLog(path);
        if (!fullLog.equals("")) {
            String[] strParts = fullLog.split(System.lineSeparator());
            String str = strParts[0].trim();
            if (!PortalControl.latestCommand.equals(str)) {
                LOGGER.info(str);
                PortalControl.latestCommand = str;
                changeMigrationStatus(str);
            }
        }
    }

    /**
     * Write input order int.
     *
     * @param command the command
     */
    public static void writeInputOrder(String command) {
        String inputOrderPath = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        File file = new File(inputOrderPath);
        try {
            if (!file.exists()) {
                Tools.createFile(inputOrderPath, true);
            }
            LogView.writeFile(command, inputOrderPath, false);
        } catch (PortalException e) {
            e.setRequestInformation("Write input order failed");
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
        }
    }

    /**
     * Clean input order.
     */
    public static void cleanInputOrder() {
        File file = new File(PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH));
        if (file.exists()) {
            try {
                FileWriter fileWriter = new FileWriter(file);
                fileWriter.write("");
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                PortalException portalException = new PortalException("IO exception", "clean input order", e.getMessage());
                portalException.setRequestInformation("Clean input order failed");
                LOGGER.error(portalException.toString());
                Tools.shutDownPortal(portalException.toString());
            }
        }
    }

    /**
     * Create file boolean.
     *
     * @param path   the path
     * @param isFile the is file
     * @throws PortalException the portal exception
     */
    public static void createFile(String path, boolean isFile) throws PortalException {
        File file = new File(path);
        if (!file.exists()) {
            try {
                if (isFile) {
                    int lastIndex = path.lastIndexOf(File.separator);
                    String folderPath = path.substring(0, lastIndex);
                    File folder = new File(folderPath);
                    folder.mkdirs();
                    file.createNewFile();
                } else {
                    file.mkdirs();
                }
            } catch (IOException e) {
                throw new PortalException("IO exception", "creating file " + path, e.getMessage());
            }
        } else {
            LOGGER.info("File " + path + " already exists.");
        }
    }

    /**
     * Gets package path.
     *
     * @param pkgPath the pkg path
     * @param pkgName the pkg name
     * @return the package path
     */
    public static String getPackagePath(String pkgPath, String pkgName) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String path = hashtable.get(pkgPath);
        String name = hashtable.get(pkgName);
        path += name;
        return path;
    }

    /**
     * Install package boolean.
     *
     * @param filePathList     the file path list
     * @param pkgPathParameter the pkg path parameter
     * @param pkgNameParameter the pkg name parameter
     * @param pkgSpace         the pkg space
     * @param installPath      the install path
     * @throws PortalException the portal exception
     */
    public static void installPackage(ArrayList<String> filePathList, String pkgPathParameter, String pkgNameParameter, String pkgSpace,
                                      String installPath) throws PortalException {
        String packagePath = Tools.getPackagePath(pkgPathParameter, pkgNameParameter);
        Tools.createFile(installPath, false);
        RuntimeExecTools.unzipFile(packagePath, pkgSpace, installPath);
        for (String path : filePathList) {
            File file = new File(path);
            if (!file.exists()) {
                throw new PortalException("Portal exception", "installing package " + packagePath, "Install package " + packagePath + " failed");
            }
        }
        Tools.outputResult(true, "Install package " + packagePath);
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
            InetAddress Address = InetAddress.getByName(host);
            Socket socket = new Socket(Address, port);
            flag = false;
            socket.close();
        } catch (UnknownHostException e) {
            PortalException portalException = new PortalException("Unknown host exception", "checking port is available", e.getMessage());
            portalException.setRequestInformation("Unknown host address.Cannot get available ports");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            LOGGER.info("The port " + host + ":" + port + " is available.");
        }
        return flag;
    }

    /**
     * Wait for incremental signal.
     *
     * @param msg the msg
     */
    public static void waitForIncrementalSignal(String msg) {
        while (true) {
            Tools.sleepThread(1000, "waiting for signal");
            if (Plan.runReverseMigration || Plan.runIncrementalMigration || Plan.stopPlan) {
                LOGGER.info(msg);
                break;
            }
        }
    }

    /**
     * Wait for reverse signal.
     *
     * @param msg the msg
     */
    public static void waitForReverseSignal(String msg) {
        while (true) {
            Tools.sleepThread(1000, "waiting for signal");
            if (Plan.runReverseMigration || Plan.stopPlan) {
                LOGGER.info(msg);
                break;
            }
        }
    }

    /**
     * Change migration status.
     *
     * @param command the command
     */
    public static void changeMigrationStatus(String command) {
        switch (command) {
            case Command.Stop.INCREMENTAL_MIGRATION: {
                Plan.stopIncrementalMigration = true;
                Plan.runIncrementalMigration = false;
                break;
            }
            case Command.Stop.REVERSE_MIGRATION: {
                Plan.stopReverseMigration = true;
                Plan.runReverseMigration = false;
                break;
            }
            case Command.Run.INCREMENTAL_MIGRATION: {
                Plan.runIncrementalMigration = true;
                Plan.stopIncrementalMigration = false;
                break;
            }
            case Command.Run.REVERSE_MIGRATION: {
                Plan.runReverseMigration = true;
                Plan.stopReverseMigration = false;
                break;
            }
            case Command.Stop.PLAN: {
                Plan.stopPlan = true;
                break;
            }
            default:
                break;
        }
    }

    /**
     * Change command line parameters.
     */
    public static void changeCommandLineParameters() {
        String checkSinkPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.CONFIG_PATH);
        String checkSourcePath = PortalControl.toolsConfigParametersTable.get(Check.Source.CONFIG_PATH);
        HashMap<String, Object> checkSinkOldTable = Tools.getYmlParameters(checkSinkPath);
        HashMap<String, Object> checkSinkTable = new HashMap<>();
        checkSinkTable.put(Check.Parameters.QUERY_DOP, Integer.parseInt(getOrDefault(Check.Sink.QUERY_DOP, checkSinkOldTable.get(Check.Parameters.QUERY_DOP).toString())));
        checkSinkTable.put(Check.Parameters.INITIAL_SIZE, Integer.parseInt(getOrDefault(Check.Sink.INITIAL_SIZE, checkSinkOldTable.get(Check.Parameters.INITIAL_SIZE).toString())));
        checkSinkTable.put(Check.Parameters.MIN_IDLE, Integer.parseInt(getOrDefault(Check.Sink.MIN_IDLE, checkSinkOldTable.get(Check.Parameters.MIN_IDLE).toString())));
        checkSinkTable.put(Check.Parameters.MAX_ACTIVE, Integer.parseInt(getOrDefault(Check.Sink.MAX_ACTIVE, checkSinkOldTable.get(Check.Parameters.MAX_ACTIVE).toString())));
        checkSinkTable.put(Check.Parameters.TIME_PERIOD, Integer.parseInt(getOrDefault(Check.Sink.TIME_PERIOD, checkSinkOldTable.get(Check.Parameters.TIME_PERIOD).toString())));
        checkSinkTable.put(Check.Parameters.NUM_PERIOD, Integer.parseInt(getOrDefault(Check.Sink.NUM_PERIOD, checkSinkOldTable.get(Check.Parameters.NUM_PERIOD).toString())));
        Tools.changeYmlParameters(checkSinkTable, checkSinkPath);
        HashMap<String, Object> checkSourceOldTable = Tools.getYmlParameters(checkSourcePath);
        HashMap<String, Object> checkSourceTable = new HashMap<>();
        checkSourceTable.put(Check.Parameters.QUERY_DOP, Integer.parseInt(getOrDefault(Check.Source.QUERY_DOP, checkSourceOldTable.get(Check.Parameters.QUERY_DOP).toString())));
        checkSourceTable.put(Check.Parameters.INITIAL_SIZE, Integer.parseInt(getOrDefault(Check.Source.INITIAL_SIZE, checkSourceOldTable.get(Check.Parameters.INITIAL_SIZE).toString())));
        checkSourceTable.put(Check.Parameters.MIN_IDLE, Integer.parseInt(getOrDefault(Check.Source.MIN_IDLE, checkSourceOldTable.get(Check.Parameters.MIN_IDLE).toString())));
        checkSourceTable.put(Check.Parameters.MAX_ACTIVE, Integer.parseInt(getOrDefault(Check.Source.MAX_ACTIVE, checkSourceOldTable.get(Check.Parameters.MAX_ACTIVE).toString())));
        checkSourceTable.put(Check.Parameters.TIME_PERIOD, Integer.parseInt(getOrDefault(Check.Source.TIME_PERIOD, checkSourceOldTable.get(Check.Parameters.TIME_PERIOD).toString())));
        checkSourceTable.put(Check.Parameters.NUM_PERIOD, Integer.parseInt(getOrDefault(Check.Source.NUM_PERIOD, checkSourceOldTable.get(Check.Parameters.NUM_PERIOD).toString())));
        Tools.changeYmlParameters(checkSourceTable, checkSourcePath);
        Tools.writeCheckRules();
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
     * Change file.
     *
     * @param oldString the old string
     * @param newString the new string
     * @param path      the path
     */
    public static void changeFile(String oldString, String newString, String path) {
        try {
            StringBuilder result = new StringBuilder();
            String temp;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            while ((temp = bufferedReader.readLine()) != null) {
                if (temp.contains(oldString)) {
                    temp = temp.replaceFirst(oldString, newString);
                }
                result.append(temp).append(System.lineSeparator());
            }
            bufferedReader.close();
            LogView.writeFile(result.toString(), path, false);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing file parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change connect xml file.
     *
     * @param workspaceIdString the workspace id string
     * @param path              the path
     */
    public static void changeConnectXmlFile(String workspaceIdString, String path) {
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
            PortalException portalException = new PortalException("IO exception", "changing xml file parameters", e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Change datacheck speed status.
     *
     * @param progressPath the progress path
     * @param statusPath   the status path
     */
    public static void changeDatacheckSpeedStatus(String progressPath, String statusPath) {
        if (new File(progressPath).exists()) {
            String progressStr = Tools.lastLine(progressPath);
            LogView.writeFile(progressStr, statusPath, false);
        } else {
            LOGGER.info("Get datacheck progess failed.Use old progress.");
        }
    }

    /**
     * Write check rules.
     */
    public static void writeCheckRules() {
        String path = PortalControl.toolsConfigParametersTable.get(Check.CONFIG_PATH);
        HashMap<String, Object> checkConfigHashMap = Tools.getYmlParameters(path);
        RuleParameter tableRuleParameter = new RuleParameter(Check.Rules.Table.AMOUNT, Check.Rules.Table.NAME,
                Check.Rules.Table.TEXT, "");
        RuleParameter rowRuleParameter = new RuleParameter(Check.Rules.Row.AMOUNT, Check.Rules.Row.NAME,
                Check.Rules.Row.TEXT, "");
        RuleParameter columnRuleParameter = new RuleParameter(Check.Rules.Column.AMOUNT, Check.Rules.Column.NAME,
                Check.Rules.Column.TEXT, Check.Rules.Column.ATTRIBUTE);
        String rulesEnableParameter = Tools.getOrDefault(Check.Rules.ENABLE, String.valueOf(checkConfigHashMap.get(Check.Rules.ENABLE)));
        checkConfigHashMap.put(Check.Rules.ENABLE, Boolean.valueOf(rulesEnableParameter));
        checkConfigHashMap = getCheckRulesFromCommandLine(checkConfigHashMap, tableRuleParameter, false);
        checkConfigHashMap = getCheckRulesFromCommandLine(checkConfigHashMap, rowRuleParameter, false);
        checkConfigHashMap = getCheckRulesFromCommandLine(checkConfigHashMap, columnRuleParameter, true);
        Tools.changeYmlParameters(checkConfigHashMap, path);
    }

    /**
     * Gets check rules from command line.
     *
     * @param hashMap       the hash map
     * @param ruleParameter the rule parameter
     * @param hasAttribute  the has attribute
     * @return the check rules from command line
     */
    public static HashMap<String, Object> getCheckRulesFromCommandLine(HashMap<String, Object> hashMap, RuleParameter ruleParameter, boolean hasAttribute) {
        ArrayList<CheckRule> checkRules = new ArrayList<>();
        String ruleAmount = ruleParameter.getAmount();
        String ruleName = ruleParameter.getName();
        String ruleText = ruleParameter.getText();
        String ruleAttribute = ruleParameter.getAttribute();
        if (System.getProperty(ruleAmount) != null) {
            int amount = Integer.parseInt(System.getProperty(ruleAmount));
            for (int i = 1; i <= amount; i++) {
                CheckRule checkRule;
                String name = System.getProperty(ruleName + i);
                String text = System.getProperty(ruleText + i);
                if (hasAttribute) {
                    String attribute = System.getProperty(ruleAttribute + i);
                    checkRule = new CheckColumnRule(name, text, attribute);
                } else {
                    checkRule = new CheckRule(name, text);
                }
                checkRules.add(checkRule);
            }
            changeCheckRules(hashMap, ruleAmount, checkRules);
        }
        return hashMap;
    }

    /**
     * Change check rules hash map.
     *
     * @param oldMap     the old map
     * @param key        the key
     * @param checkRules the check rules
     * @return the hash map
     */
    public static HashMap<String, Object> changeCheckRules(HashMap<String, Object> oldMap, String key, ArrayList<CheckRule> checkRules) {
        ArrayList<Object> objectArrayList = new ArrayList<>();
        for (CheckRule checkRule : checkRules) {
            Object jsonObject = JSON.toJSON(checkRule);
            objectArrayList.add(jsonObject);
        }
        if (oldMap.containsKey(key)) {
            oldMap.replace(key, objectArrayList);
        } else {
            oldMap.put(key, objectArrayList);
        }
        return oldMap;
    }

    /**
     * Write chameleon override type.
     */
    public static void writeChameleonOverrideType() {
        String path = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
        HashMap<String, Object> oldChameleonConfigMap = Tools.getYmlParameters(path);
        if (System.getProperty(Chameleon.Override.AMOUNT) != null) {
            int amount = Integer.parseInt(System.getProperty(Chameleon.Override.AMOUNT));
            oldChameleonConfigMap.remove(Chameleon.Override.AMOUNT);
            for (int i = 1; i <= amount; i++) {
                String sourceType = System.getProperty(Chameleon.Override.SOURCE_TYPE + i);
                String sinkType = System.getProperty(Chameleon.Override.SINK_TYPE + i);
                String tables = System.getProperty(Chameleon.Override.TABLES + i);
                String[] tableArray;
                if (tables.contains(",")) {
                    tableArray = tables.split(",");
                } else {
                    tableArray = new String[]{tables};
                }
                HashMap<String, Object> typeOverrideHashMap = new HashMap<>();
                typeOverrideHashMap.put(Chameleon.Override.SINK_TYPE, sinkType);
                typeOverrideHashMap.put(Chameleon.Override.TABLES, tableArray);
                oldChameleonConfigMap.put(Chameleon.Override.AMOUNT + "." + sourceType, typeOverrideHashMap);
            }
        }
        Tools.changeYmlParameters(oldChameleonConfigMap, path);
    }

    /**
     * Joint chameleon orders string.
     *
     * @param chameleonParameterTable the chameleon parameter table
     * @param order                   the order
     * @return the string
     */
    public static String jointChameleonOrders(Hashtable<String, String> chameleonParameterTable, String order) {
        String chameleonFile = PortalControl.toolsConfigParametersTable.get(Chameleon.RUNNABLE_FILE_PATH);
        StringBuilder chameleonOrder = new StringBuilder(chameleonFile + " " + order + " ");
        for (String key : chameleonParameterTable.keySet()) {
            chameleonOrder.append(key).append(" ").append(chameleonParameterTable.get(key)).append(" ");
        }
        return chameleonOrder.substring(0, chameleonOrder.length() - 1);
    }

    /**
     * Output file string string.
     *
     * @param path the path
     * @return the string
     */
    public static String outputFileString(String path) {
        StringBuilder str = new StringBuilder();
        try {
            BufferedReader fileReader = new BufferedReader((new InputStreamReader(new FileInputStream(path))));
            String tempStr;
            while ((tempStr = fileReader.readLine()) != null) {
                str.append(tempStr).append(System.lineSeparator());
                LOGGER.warn(tempStr);
            }
            fileReader.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "output strings in file " + path, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
        }
        return str.toString();
    }

    /**
     * Gets parameter command line first.
     *
     * @param hashtable the hashtable
     * @param path      the path
     */
    public static void getParameterCommandLineFirst(Hashtable<String, String> hashtable, String path) {
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            Properties pps = new Properties();
            try {
                pps.load(new FileInputStream(path));
            } catch (IOException e) {
                PortalException portalException = new PortalException("IO exception", "loading the parameters in file " + path, e.getMessage());
                LOGGER.error(portalException.toString());
                Tools.shutDownPortal(portalException.toString());
                return;
            }
            for (Object key : pps.keySet()) {
                String keyString = String.valueOf(key);
                String valueString = System.getProperty(keyString);
                if (valueString == null) {
                    valueString = pps.getProperty(keyString);
                }
                if (keyString.contains("path") && !valueString.endsWith(File.separator)) {
                    valueString += File.separator;
                }
                hashtable.put(keyString, valueString);
            }
            pps.clear();
            for (String key : hashtable.keySet()) {
                String valueString = hashtable.get(key);
                hashtable.replace(key, changeValue(valueString, hashtable));
            }
            Tools.changePropertiesParameters(hashtable, path);
        }
    }

    /**
     * Output result.
     *
     * @param flag  the flag
     * @param order the order
     */
    public static void outputResult(boolean flag, String order) {
        if (flag) {
            LOGGER.info(order + " success.");
        } else {
            LOGGER.error("Error message: " + order + " failed.");
        }
    }

    /**
     * Output information.
     *
     * @param flag        the flag
     * @param trueString  the true string
     * @param falseString the false string
     */
    public static void outputInformation(boolean flag, String trueString, String falseString) {
        if (flag) {
            LOGGER.info(trueString);
        } else if (!falseString.equals("")) {
            LOGGER.error(falseString);
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
            Tools.createFile(log, true);
        } catch (PortalException e) {
            e.setRequestInformation("Create file failed.Please ensure the file " + log + " is available to check whether the curl order finishes successfully.");
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
            return;
        }
        String config = Tools.getSinglePropertiesParameter("key.converter.schema.registry.url", configFile);
        config += "/config";
        String[] cmdParts = new String[]{"curl", "-X", "PUT", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data", "{\"compatibility\": \"NONE\"}", config};
        try {
            RuntimeExecTools.executeOrderCurrentRuntime(cmdParts, 1000, log, "Run curl failed.");
        } catch (PortalException e) {
            e.setRequestInformation("Run curl failed.");
            LOGGER.error(e.toString());
            Tools.shutDownPortal(e.toString());
        }
    }

    /**
     * Sets port id.
     *
     * @param name the name
     * @return the port id
     */
    public static int setPortId(String name) {
        int portId = -1;
        try {
            File pidFile = new File(name);
            RandomAccessFile randomAccessFile = new RandomAccessFile(pidFile, "rw");
            FileInputStream fileInputStream = new FileInputStream(pidFile);
            FileChannel channel = randomAccessFile.getChannel();
            FileLock lock = channel.tryLock();
            if (lock != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
                String idString = br.readLine();
                portId = idString == null ? 0 : Integer.parseInt(idString.trim());
                br.close();
                portId++;
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pidFile)));
                bw.write(String.valueOf(portId));
                bw.flush();
                bw.close();
                lock.release();
                lock.close();
            }
            channel.close();
            fileInputStream.close();
            randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Error massage: Get lock failed.");
        }
        return portId;
    }

    /**
     * Output datacheck status boolean.
     *
     * @param datacheckType the datacheck type
     */
    public static void outputDatacheckStatus(String datacheckType) {
        String checkSourceLogPath = PortalControl.toolsConfigParametersTable.get(Check.Source.LOG_PATH);
        boolean flag1 = Tools.outputStatus(checkSourceLogPath);
        String checkSinkLogPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.LOG_PATH);
        boolean flag2 = Tools.outputStatus(checkSinkLogPath);
        String checkLogPath = PortalControl.toolsConfigParametersTable.get(Check.LOG_PATH);
        boolean flag3 = Tools.outputStatus(checkLogPath);
        boolean flag = flag1 && flag2 && flag3;
        Tools.outputInformation(flag, datacheckType + " is running.", datacheckType + " has error.");
    }

    /**
     * Output status boolean.
     *
     * @param logPath the log path
     * @return the boolean
     */
    public static boolean outputStatus(String logPath) {
        boolean flag = true;
        if (new File(logPath).exists()) {
            String errorStr = LogView.getErrorMsg(logPath);
            if (!errorStr.equals("")) {
                LOGGER.error(errorStr);
                LOGGER.error("Error occurred in " + logPath + ".You can stop plan or ignore the information.");
                flag = false;
            }
        }
        return flag;
    }

    /**
     * Sleep thread.
     *
     * @param time the time
     * @param name the name
     */
    public static void sleepThread(int time, String name) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted exception occurred in " + name + ".");
        }
    }

    /**
     * Check reverse migration runnable boolean.
     *
     * @return the boolean
     */
    public static boolean checkReverseMigrationRunnable() {
        boolean isReverseRunnable = false;
        try (PgConnection connection = JdbcTools.getPgConnection()) {
            Hashtable<String, String> parameterTable = new Hashtable<>();
            parameterTable.put("wal_level", "logical");
            int parameter = 0;
            for (String key : parameterTable.keySet()) {
                if (JdbcTools.selectGlobalVariables(connection, key, parameterTable.get(key))) {
                    parameter++;
                } else {
                    break;
                }
            }
            if (parameter == parameterTable.size()) {
                isReverseRunnable = true;
            }
        } catch (SQLException e) {
            PortalException portalException = new PortalException("IO exception",
                    "checking reverse migration is runnable", e.getMessage());
            PortalControl.refuseReverseMigrationReason = portalException.getMessage();
            LOGGER.error(portalException.toString());
        }
        return isReverseRunnable;
    }

    /**
     * Sets x log path.
     */
    public static void setXLogPath() {
        String xLogPath = PortalControl.toolsConfigParametersTable.get(Status.XLOG_PATH);
        String xLogLocation = "";
        File file = new File(xLogPath);
        try {
            if (file.exists()) {
                BufferedReader fileReader = new BufferedReader((new InputStreamReader(new FileInputStream(file))));
                String tempStr;
                while ((tempStr = fileReader.readLine()) != null) {
                    if (tempStr.contains("xlog location")) {
                        int index = tempStr.lastIndexOf(":") + 1;
                        xLogLocation = tempStr.substring(index).trim();
                    }
                }
                fileReader.close();
            }
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "reading xlog.path in file " + file.getAbsolutePath(), e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
            return;
        }
        String configPath = PortalControl.toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONFIG_PATH);
        Tools.changeSinglePropertiesParameter("xlog.location", xLogLocation, configPath);
    }

    /**
     * Is full datacheck success boolean.
     *
     * @return the boolean
     */
    public static boolean isFullDatacheckSuccess() {
        boolean runningFullDatacheck = PortalControl.status >= Status.START_FULL_MIGRATION_CHECK;
        String checkPath = PortalControl.toolsConfigParametersTable.get(Check.LOG_PATH);
        return runningFullDatacheck && LogView.checkCheckSuccessLogFlag(checkPath);
    }

    /**
     * Combain order string.
     *
     * @param parts the parts
     * @return the string
     */
    public static String combainOrder(String[] parts) {
        StringBuilder path;
        path = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            path.append(" ").append(parts[i]);
        }

        return path.toString();
    }

    /**
     * Shut down portal.
     *
     * @param str the str
     */
    public static void shutDownPortal(String str) {
        Plan.stopPlan = true;
        PortalControl.status = Status.ERROR;
        PortalControl.errorMsg = str;
    }

    /**
     * Contain string boolean.
     *
     * @param order the order
     * @param key   the key
     * @return the boolean
     */
    public static boolean containString(String order, String key) {
        String[] orderPart = order.split(" ");
        for (String part : orderPart) {
            if (part.equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets current portal pid.
     *
     * @return the current portal pid
     */
    public static int getCurrentPortalPid() {
        int pid = -1;
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName();
        int index = name.indexOf("@");
        if (index != -1) {
            pid = Integer.parseInt(name.substring(0, index));
        }
        return pid;
    }

    /**
     * Change kafka parameters.
     */
    public static void changeKafkaParameters() {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        Tools.changeSinglePropertiesParameter("dataDir", hashtable.get(Debezium.Zookeeper.TMP_PATH),
                hashtable.get(Debezium.Zookeeper.CONFIG_PATH));
        Hashtable<String, String> kafkaConfigTable = new Hashtable<>();
        kafkaConfigTable.put("log.dirs", hashtable.get(Debezium.Kafka.TMP_PATH));
        kafkaConfigTable.put("zookeeper.connection.timeout.ms", "30000");
        kafkaConfigTable.put("zookeeper.session.timeout.ms", "30000");
        kafkaConfigTable.put("delete.topic.enable", "true");
        kafkaConfigTable.put("group.initial.rebalance.delay.ms", "0");
        kafkaConfigTable.put("num.network.threads", "8");
        kafkaConfigTable.put("num.io.threads", "16");
        Tools.changePropertiesParameters(kafkaConfigTable, hashtable.get(Debezium.Kafka.CONFIG_PATH));
    }

    /**
     * Prepare confluent.
     */
    public static void prepareConfluent() {
        String workDirectory = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        String cleanFileName = "clean.sh";
        RuntimeExecTools.runShell(cleanFileName, workDirectory);
        String buildFileName = "build.sh";
        RuntimeExecTools.runShell(buildFileName, workDirectory);
    }


    /**
     * Start kafka.
     */
    public static void startKafka() {
        PortalControl.initHashTable();
        Task.initRunTaskHandlerHashMap();
        changeKafkaParameters();
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String confluentPath = hashtable.get(Debezium.Confluent.PATH);
        String configPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "config",
                "migrationConfig.properties");
        Task.startTaskMethod(Method.Name.ZOOKEEPER, 2000, "", "");
        String executeKafkaFile = PathUtils.combainPath(true, confluentPath + "bin", "kafka-topics");
        String kafkaPort = Tools.getSinglePropertiesParameter(Parameter.Port.KAFKA, configPath);
        String kafkaOrder = executeKafkaFile + " --list --bootstrap-server " + kafkaPort;
        Task.startTaskMethod(Method.Name.KAFKA, 10000, kafkaOrder, "Broker may not be available.");
        Task.startTaskMethod(Method.Name.REGISTRY, 3000, "", "");
        if (!checkKafkaProcess()) {
            return;
        }
        LOGGER.info("Start kafka success.");
    }

    /**
     * check kafka process status
     *
     * @return boolean
     */
    public static boolean checkKafkaProcess() {
        ArrayList<String> stringArrayList = new ArrayList<>();
        stringArrayList.add(Method.Run.ZOOKEEPER);
        stringArrayList.add(Method.Run.KAFKA);
        stringArrayList.add(Method.Run.REGISTRY);
        for (String methodName : stringArrayList) {
            if (Tools.getCommandPid(Task.getTaskProcessMap().get(methodName)) == -1) {
                LOGGER.error("Start kafka failed.");
                return false;
            }
        }
        return true;
    }

    /**
     * Stop kafka.
     */
    public static void stopKafka() {
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
    }

    /**
     * Wait for kill kafka.
     */
    public static void waitForKillKafka() {
        int waitTime = 5000;
        while (true) {
            long pid = Tools.getCommandPid(Task.getTaskProcessMap().get(Method.Run.KAFKA));
            if (pid == -1) {
                break;
            } else if (waitTime < 0) {
                killKafka(pid);
                break;
            } else {
                Tools.sleepThread(1000, "stop kafka");
                waitTime -= 1000;
            }
        }
    }

    /**
     * Kill kafka.
     *
     * @param pid the pid
     */
    public static void killKafka(long pid) {
        try {
            LOGGER.warn("Force kill on process kafka.");
            RuntimeExecTools.executeOrder("kill -9 " + pid, 1000, PortalControl.portalErrorPath);
            String tmpPath = PortalControl.portalControlPath + "tmp";
            String tmpZookeeperPath = PathUtils.combainPath(false, tmpPath, "zookeeper");
            RuntimeExecTools.executeOrder("rm -rf " + tmpZookeeperPath, 1000, PortalControl.portalErrorPath);
            String tmpKafkaPath = PathUtils.combainPath(false, tmpPath, "kafka-logs");
            RuntimeExecTools.executeOrder("rm -rf " + tmpKafkaPath, 1000, PortalControl.portalErrorPath);
        } catch (PortalException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Change datacheck log path.
     *
     * @param logPatternFile the log pattern file
     */
    public static void changeDatacheckLogPath(String logPatternFile) {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        String path = hashtable.get(logPatternFile);
        String log = LogView.getFullLog(path);
        String logHome = "<Property name=\"LOG_HOME\">";
        String datacheck = "datacheck";
        String logs = "logs";
        for (String str : log.split(System.lineSeparator())) {
            if (str.contains(logHome)) {
                String workspacePath = WorkspacePath.getInstance(PortalControl.portalControlPath, Plan.workspaceId)
                        .getWorkspaceLogPath();
                String dataCheckLogPath = PathUtils.combainPath(true, workspacePath, datacheck);
                Tools.changeFile(logs, dataCheckLogPath, path);
            }
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
        String newString = oldString;
        while (newString.contains("$")) {
            String variable = newString.substring(newString.indexOf("$"), newString.indexOf("}") + 1);
            String variableName = newString.substring(newString.indexOf("{") + 1, newString.indexOf("}"));
            String value = hashtable.get(variableName);
            newString = newString.replace(variable, value);
        }
        return newString;
    }

    /**
     * Check system info.
     *
     * @throws PortalException the portal exception
     */
    public static void checkSystemInfo() throws PortalException {
        Hashtable<String, String> hashtable = PortalControl.toolsConfigParametersTable;
        checkSystemArch(hashtable.get(Parameter.SYSTEM_ARCH));
        checkSystemName(hashtable.get(Parameter.SYSTEM_NAME));
    }

    /**
     * Gets system name.
     *
     * @param parameterSystemName the parameter system name
     * @throws PortalException the portal exception
     */
    public static void checkSystemName(String parameterSystemName) throws PortalException {
        String filePath = PathUtils.combainPath(true, "", "etc", "os-release");
        String fullStr = LogView.getFullLog(filePath);
        String id = "";
        String versionId = "";
        for (String str : fullStr.split(System.lineSeparator())) {
            if (str.startsWith("ID=")) {
                id = str.substring(str.indexOf("\"") + 1, str.lastIndexOf("\""));
            }
            if (str.startsWith("VERSION_ID=")) {
                versionId = str.substring(str.indexOf("\"") + 1, str.lastIndexOf("\""));
            }
        }
        String systemName = id + versionId;
        if (!systemName.equals(parameterSystemName)) {
            throw new PortalException("Portal exception", "unzip package",
                    "The parameter " + Parameter.SYSTEM_NAME + " doesn't match your system",
                    "The parameter " + Parameter.SYSTEM_NAME
                            + " doesn't match your system,please check the value of parameter "
                            + Parameter.SYSTEM_NAME);
        }
    }

    /**
     * Gets system name.
     *
     * @param parameterSystemArch the parameter system arch
     * @throws PortalException the portal exception
     */
    public static void checkSystemArch(String parameterSystemArch) throws PortalException {
        String versionStr = RuntimeExecTools.executeOrder("uname -a", 1000);
        String[] versionParts = versionStr.split(" ");
        String systemArch = versionParts[versionParts.length - 2];
        if (!systemArch.equals(parameterSystemArch)) {
            throw new PortalException("Portal exception", "unzip package",
                    "The parameter " + Parameter.SYSTEM_ARCH + " doesn't match your system",
                    "The parameter " + Parameter.SYSTEM_ARCH
                            + " doesn't match your system,please check the value of parameter "
                            + Parameter.SYSTEM_ARCH);
        }
    }

    /**
     * Check and wait.
     *
     * @param time           the time
     * @param isTaskFinished the is task finished
     * @param information    the information
     */
    public static void checkAndWait(int time, boolean isTaskFinished, String information) {
        int maxTime = time;
        while (maxTime > 0) {
            Tools.sleepThread(1000, "waiting for process running");
            if (isTaskFinished) {
                break;
            }
            maxTime--;
        }
        if (maxTime <= 0) {
            LOGGER.error("{} timed out.", information);
        }
    }
}
