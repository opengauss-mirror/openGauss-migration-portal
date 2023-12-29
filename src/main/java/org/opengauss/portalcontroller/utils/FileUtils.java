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
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
            LogViewUtils.writeFile(command, inputOrderPath, false);
        } catch (PortalException e) {
            e.setRequestInformation("Write input order failed");
            log.error(e.toString());
            PortalControl.shutDownPortal(e.toString());
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
                PortalException portalException = new PortalException("IO exception", "clean input order",
                        e.getMessage());
                portalException.setRequestInformation("Clean input order failed");
                log.error(portalException.toString());
                PortalControl.shutDownPortal(portalException.toString());
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
            LogViewUtils.writeFile(result.toString(), path, false);
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "changing file parameters",
                    e.getMessage());
            log.error(portalException.toString());
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
            log.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
        return str.toString();
    }
}
