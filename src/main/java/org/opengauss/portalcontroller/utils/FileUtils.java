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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.springframework.util.ObjectUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * FileUtils
 *
 * @author: www
 * @date: 2023/11/28 11:14
 * @description: FileUtils
 * @since: 1.1
 * @version: 1.1
 */
@Slf4j
public class FileUtils {
    /**
     * check file exist
     *
     * @param filePath filePath
     * @param timeout  timeout
     */
    public static void checkFileExist(String filePath, int timeout) {
        int timeOutCount = 0;
        while (!(new File(filePath).exists()) && timeOutCount < timeout) {
            try {
                TimeUnit.SECONDS.sleep(1);
                log.info("check file exist sleep : {} s", timeOutCount);
            } catch (InterruptedException e) {
                log.error("sleep exception:", e);
            }
            timeOutCount++;
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
                createFile(inputOrderPath, true);
            }
            String orderWithTimestamp = generateOrderWithTimestamp(command);
            writeFile(orderWithTimestamp, inputOrderPath, false);
        } catch (PortalException e) {
            e.setRequestInformation("Write input order failed");
            log.error("{}Write input order to file failed, file path: {}", ErrorCode.IO_EXCEPTION, inputOrderPath, e);
            PortalControl.shutDownPortal(e.toString());
        }
    }

    /**
     * generate order with timestamp
     *
     * @param command order
     * @return order with time stamp
     */
    public static String generateOrderWithTimestamp(String command) {
        String orderInvokedTimestamp = System.getProperty(Parameter.ORDER_INVOKED_TIMESTAMP);
        if (ObjectUtils.isEmpty(orderInvokedTimestamp)) {
            orderInvokedTimestamp = String.valueOf(System.currentTimeMillis());
        }

        return String.format("%s:%s", command, orderInvokedTimestamp);
    }

    /**
     * parse order with timestamp
     *
     * @param orderWithTimestamp order with timestamp
     * @return Map
     */
    public static Map<String, String> parseOrderWithTimestamp(String orderWithTimestamp) {
        HashMap<String, String> result = new HashMap<>();

        int index = orderWithTimestamp.indexOf(":");
        String order = index != -1 ? orderWithTimestamp.substring(0, index) : orderWithTimestamp;
        String orderInvokedTimestamp = index != -1 ? orderWithTimestamp.substring(index + 1) : "0";

        result.put(Command.Parameters.ORDER, order);
        result.put(Parameter.ORDER_INVOKED_TIMESTAMP, orderInvokedTimestamp);
        return result;
    }

    /**
     * Clean input order.
     */
    public static void cleanInputOrder() {
        String path = PortalControl.toolsConfigParametersTable.get(Parameter.INPUT_ORDER_PATH);
        File file = new File(path);
        String fullLog = LogViewUtils.getFullLog(path);

        if (!file.exists() || fullLog.isEmpty()) {
            return;
        }

        String[] strParts = fullLog.split(System.lineSeparator());
        Map<String, String> orderMap = FileUtils.parseOrderWithTimestamp(strParts[0].trim());
        String oldOrder = orderMap.get(Command.Parameters.ORDER);
        long oldOrderInvokedTimestamp = Long.parseLong(orderMap.get(Parameter.ORDER_INVOKED_TIMESTAMP));

        if (oldOrder.equals(Command.Stop.PLAN)
                && !ObjectUtils.isEmpty(System.getProperty(Parameter.ORDER_INVOKED_TIMESTAMP))
                && Long.parseLong(System.getProperty(Parameter.ORDER_INVOKED_TIMESTAMP)) < oldOrderInvokedTimestamp) {
            return;
        }

        writeFile("", path, false);
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
            log.info("File " + path + " already exists.");
        }
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
            writeFile(result.toString(), path, false);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing file parameters",
                    e.getMessage());
            log.error("{}Failed to modify file content, file path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
    }

    /**
     * Output file string string.
     *
     * @param path the path
     * @return the string
     */
    public static String outputFileString(String path) {
        if (!Files.exists(Path.of(path))) {
            String result = String.format("File {} does not exist", path);
            log.warn(result);
            return result;
        }
        StringBuilder str = new StringBuilder();
        try {
            BufferedReader fileReader = new BufferedReader((new InputStreamReader(new FileInputStream(path))));
            String tempStr;
            while ((tempStr = fileReader.readLine()) != null) {
                str.append(tempStr).append(System.lineSeparator());
                log.warn(tempStr);
            }
            fileReader.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "output strings in file " + path,
                    e.getMessage());
            log.error("{}Failed to output strings to file, file path: {}", ErrorCode.IO_EXCEPTION, path, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        return str.toString();
    }

    /**
     * Write file.
     *
     * @param stringList the string list
     * @param path       the path
     * @param append     the append
     */
    public static void writeFile(List<String> stringList, String path, boolean append) {
        StringBuilder str = new StringBuilder();
        for (String tempStr : stringList) {
            str.append(tempStr).append(System.lineSeparator());
        }
        writeFile(str.toString(), path, append);
    }

    /**
     * Write file.
     *
     * @param str    the str
     * @param path   the path
     * @param append the append
     */
    public static void writeFile(String str, String path, boolean append) {
        if (append) {
            writeAppendFile(path, str);
        } else {
            writeFileRest(path, str);
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeAppendFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Write file rest.
     *
     * @param filename the filename
     * @param content  the content
     */
    public static void writeFileRest(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * delete a specified file or directory
     *
     * @param path file path
     */
    public static void removeFileOrDirectory(String path) {
        String command = String.format("rm -rf %s", path);
        try {
            RuntimeExecUtils.executeOrder(command, 1000);
        } catch (PortalException e) {
            log.error("Remove file '{}' failed.", path, e);
        }
    }
}
