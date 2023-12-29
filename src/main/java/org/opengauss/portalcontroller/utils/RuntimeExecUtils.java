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

package org.opengauss.portalcontroller.utils;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * RuntimeExecUtils
 *
 * @date :2024/1/15 11:38
 * @description: RuntimeExecUtils
 * @version: 1.1
 * @since 1.1
 */
public class RuntimeExecUtils {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RuntimeExecUtils.class);

    /**
     * Execute order long.
     *
     * @param command       the command
     * @param time          the time
     * @param errorFilePath the error file path
     * @throws PortalException the portal exception
     */
    public static void executeOrder(String command, int time, String errorFilePath) throws PortalException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String[] commands = command.split(" ");
        processBuilder.command(commands);
        try {
            Process process = processBuilder.start();
            process.waitFor(time, TimeUnit.MILLISECONDS);
            String errorStr = getInputStreamString(process.getErrorStream());
            if (!errorStr.equals("")) {
                LOGGER.warn("Error command:" + command);
                LOGGER.error(errorStr);
            }
            LogViewUtils.writeFile(errorStr, errorFilePath, true);
        } catch (IOException e) {
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        }
    }

    /**
     * Execute order string.
     *
     * @param command the command
     * @param time    the time
     * @return the string
     * @throws PortalException the portal exception
     */
    public static String executeOrder(String command, int time) throws PortalException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String[] commands = command.split(" ");
        processBuilder.command(commands);
        String result = "";
        try {
            Process process = processBuilder.start();
            process.waitFor(time, TimeUnit.MILLISECONDS);
            result = getInputStreamString(process.getInputStream());
        } catch (IOException e) {
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        }
        return result;
    }

    /**
     * Execute order.
     *
     * @param command            the command
     * @param time               the time
     * @param workDirectory      the work directory
     * @param errorFilePath      the error file path
     * @param shouldChangeOutPut the should change output
     * @param outPutStringList   the output string list
     * @throws PortalException the portal exception
     */
    public static void executeOrder(String command, int time, String workDirectory, String errorFilePath,
                                    boolean shouldChangeOutPut, ArrayList<String> outPutStringList)
            throws PortalException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String[] commands = command.split(" ");
        processBuilder.directory(new File(workDirectory));
        processBuilder.command(commands);
        if (shouldChangeOutPut) {
            processBuilder.redirectErrorStream(true);
            processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(errorFilePath)));
        } else {
            processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File(errorFilePath)));
        }
        try {
            Process process = processBuilder.start();
            if (time == 0) {
                int retCode = process.waitFor();
                if (retCode == 0) {
                    LOGGER.info("Execute order finished.");
                } else {
                    String errorStr = getInputStreamString(process.getErrorStream());
                    if (!errorStr.equals("")) {
                        LOGGER.error(errorStr);
                    }
                }
            } else {
                process.waitFor(time, TimeUnit.MILLISECONDS);
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(),
                        StandardCharsets.UTF_8))) {
                    for (String outputOrder : outPutStringList) {
                        bw.write(outputOrder);
                        ProcessUtils.sleepThread(1000, "input parameters");
                    }
                }
            }
        } catch (IOException e) {
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        }
    }

    /**
     * Execute order current runtime.
     *
     * @param cmdParts       the cmd parts
     * @param time           the time
     * @param outputFilePath the output file path
     * @param errorLog       the error log
     * @throws PortalException the portal exception
     */
    public static void executeOrderCurrentRuntime(String[] cmdParts, int time, String outputFilePath,
                                                  String errorLog) throws PortalException {
        try {
            Process process = Runtime.getRuntime().exec(cmdParts);
            String errorStr = getInputStreamString(process.getErrorStream());
            if (time == 0) {
                int retCode = process.waitFor();
                if (retCode == 0) {
                    LOGGER.info("Execute order finished.");
                } else {
                    LOGGER.error(errorStr);
                }
            } else {
                process.waitFor(time, TimeUnit.MILLISECONDS);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String str = bufferedReader.readLine();
                bufferedReader.close();
                if (str != null && !str.equals("")) {
                    LogViewUtils.writeFile(str, outputFilePath, true);
                } else {
                    LOGGER.error(errorLog);
                }
                LogViewUtils.writeFile(errorStr, outputFilePath, true);
            }
        } catch (IOException e) {
            String command = CommandUtils.combainOrder(cmdParts);
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            String command = CommandUtils.combainOrder(cmdParts);
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        }
    }

    /**
     * Download boolean.
     *
     * @param urlParameter  the url parameter
     * @param pathParameter the path parameter
     * @throws PortalException the portal exception
     */
    public static void download(String urlParameter, String pathParameter) throws PortalException {
        String url = PortalControl.toolsConfigParametersTable.get(urlParameter);
        String path = PortalControl.toolsConfigParametersTable.get(pathParameter);
        String[] urlParameters = url.split(File.separator);
        String packageName = urlParameters[urlParameters.length - 1];
        try {
            FileUtils.createFile(path, false);
            File file = new File(path + packageName);
            if (file.exists() && file.isFile()) {
                LOGGER.info("File " + path + packageName + " already exists.Skip the download package.");
            } else if (file.exists()) {
                LOGGER.error("Directory " + path + packageName + " already exists.Please rename the directory.");
            } else {
                String command = "wget -c -P " + path + " " + url + " --no-check-certificate";
                executeOrder(command, 600000, PortalControl.portalErrorPath);
                LOGGER.info("Download file " + url + " to " + path + " finished.");
            }
        } catch (PortalException e) {
            e.setRequestInformation("Cannot download package " + packageName + " to destination folder");
            e.setRepairTips("change the value of " + pathParameter + " or " + urlParameter);
            throw e;
        }
    }

    /**
     * Gets input stream string.
     *
     * @param in the in
     * @return the input stream string
     * @throws PortalException the portal exception
     */
    public static String getInputStreamString(InputStream in) throws PortalException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String str;
        StringBuilder sb = new StringBuilder();
        try {
            while ((str = br.readLine()) != null) {
                sb.append(str).append(System.lineSeparator());
            }
            br.close();
        } catch (IOException e) {
            throw new PortalException("IO exception", "getting error stream information", e.getMessage());
        }
        return sb.toString();
    }

    /**
     * Copy file.
     *
     * @param filePath  the file path
     * @param directory the directory
     * @param recovery  the recovery
     * @throws PortalException the portal exception
     */
    public static void copyFile(String filePath, String directory, boolean recovery) throws PortalException {
        File file = new File(filePath);
        if (file.exists()) {
            String fileName = file.getName();
            String newFilePath = directory + fileName;
            boolean isExist = new File(newFilePath).exists();
            if (!isExist || recovery) {
                String command = "cp -R " + filePath + " " + directory;
                executeOrder(command, 60000, PortalControl.portalErrorPath);
            }
        } else {
            LOGGER.error("File " + filePath + " not exist.");
        }
    }

    /**
     * Copy files to a new directory If the files are already in the new directory, the files will not be overwritten
     *
     * @param filePath  the file path
     * @param directory the directory
     * @throws PortalException the portal exception
     */
    public static void copyFileIfNotExist(String filePath, String directory) throws PortalException {
        File file = new File(filePath);
        if (file.exists()) {
            String fileName = file.getName();
            String newFilePath = directory + fileName;
            boolean isExist = new File(newFilePath).exists();
            if (!isExist) {
                String command = "cp -R -n " + filePath + " " + directory;
                executeOrder(command, 60000, PortalControl.portalErrorPath);
            }
        } else {
            LOGGER.error("File " + filePath + " not exist.");
        }
    }

    /**
     * Remove file.
     *
     * @param path          the path
     * @param errorFilePath the error file path
     * @throws PortalException the portal exception
     */
    public static void removeFile(String path, String errorFilePath) throws PortalException {
        if (new File(path).exists()) {
            String command = "rm -rf " + path;
            executeOrder(command, 60000, errorFilePath);
            LOGGER.info("Remove file " + path + " finished.");
        } else {
            LOGGER.info("No file " + path + " to remove.");
        }
    }

    /**
     * Unzip file.
     *
     * @param packagePath the package path
     * @param pkgSize     the pkg size
     * @param directory   the directory
     * @throws PortalException the portal exception
     */
    public static void unzipFile(String packagePath, String pkgSize, String directory) throws PortalException {
        String command;
        if (!new File(packagePath).exists()) {
            throw new PortalException("Portal exception", "unzip package", "No package to install",
                    "No package to install,please check the location of package " + packagePath);
        } else {
            checkDiskSpace(directory, pkgSize, packagePath);
        }
        if (packagePath.endsWith(".zip")) {
            command = "unzip -q -o " + packagePath + " -d " + directory;
            executeOrder(command, 900000, PortalControl.portalErrorPath);
        } else if (packagePath.endsWith(".tar.gz") || packagePath.endsWith(".tgz")) {
            command = "tar -zxf " + packagePath + " -C " + directory;
            executeOrder(command, 900000, PortalControl.portalErrorPath);
        } else {
            throw new PortalException("Portal exception", "unzip package", "Invalid package type",
                    "Invalid package type,please check if the package is ends with .zip or .tar.gz or .tgz");
        }
        LOGGER.info("Unzip file " + packagePath + " to " + directory + " finished.");
    }

    /**
     * Rename.
     *
     * @param oldName the old name
     * @param newName the new name
     * @throws PortalException the portal exception
     */
    public static void rename(String oldName, String newName) throws PortalException {
        String command = "mv " + oldName + " " + newName;
        if (new File(oldName).exists()) {
            executeOrder(command, 600000, PortalControl.portalErrorPath);
        }
        LOGGER.info("Rename file " + oldName + " to " + newName + " finished.");
    }

    /**
     * Copy file start with word.
     *
     * @param file          the file
     * @param workDirectory the work directory
     * @param criticalWord  the critical word
     * @param replaceWord   the replace word
     * @param recovery      the recovery
     * @throws PortalException the portal exception
     */
    public static void copyFileStartWithWord(File file, String workDirectory, String criticalWord, String replaceWord,
                                             boolean recovery) throws PortalException {
        if (file.getName().startsWith(criticalWord)) {
            RuntimeExecUtils.copyFile(file.getAbsolutePath(), workDirectory + replaceWord, recovery);
        }
    }

    /**
     * Execute start order.
     *
     * @param command            the command
     * @param time               the time
     * @param workDirectory      the work directory
     * @param errorFilePath      the error file path
     * @param shouldChangeOutput the should change output
     * @param information        the information
     */
    public static void executeStartOrder(String command, int time, String workDirectory, String errorFilePath,
                                         boolean shouldChangeOutput, String information) {
        LOGGER.info("start command = {}", command);
        try {
            if (!workDirectory.equals("")) {
                RuntimeExecUtils.executeOrder(command, time, workDirectory, errorFilePath, shouldChangeOutput,
                        new ArrayList<>());
            } else {
                RuntimeExecUtils.executeOrder(command, time, errorFilePath);
            }
            LOGGER.info(information + ".");
        } catch (PortalException e) {
            e.setRequestInformation(information + " failed");
            LOGGER.error(e.toString());
            PortalControl.shutDownPortal(e.toString());
        }
    }

    /**
     * Run shell.
     *
     * @param name          the name
     * @param workDirectory the work directory
     */
    public static void runShell(String name, String workDirectory) {
        if (new File(workDirectory + name).exists()) {
            String errorFilePath = PortalControl.portalErrorPath;
            try {
                String command = "sh " + name;
                RuntimeExecUtils.executeOrder(command, 3000, workDirectory, errorFilePath, true, new ArrayList<>());
            } catch (PortalException e) {
                LOGGER.error(e.getMessage());
            }
        }
        ProcessUtils.sleepThread(1000, "run shell");
    }

    /**
     * Check disk space.
     *
     * @param directory   the directory
     * @param pkgSize     the pkg size
     * @param packagePath the package path
     * @throws PortalException the portal exception
     */
    public static void checkDiskSpace(String directory, String pkgSize, String packagePath) throws PortalException {
        String sizeText = executeOrder("df -m " + directory, 1000);
        sizeText = sizeText.substring(sizeText.indexOf(System.lineSeparator()) + 1).replaceAll(" +", " ");
        String[] texts = sizeText.split(" ");
        String diskSize = texts[3];
        ProcessUtils.sleepThread(1000, "remove size file");
        int size = Integer.parseInt(pkgSize.replaceAll("MB", ""));
        if (Integer.parseInt(diskSize) < size) {
            String standardExceptionMessage = "No space left on device,install the package " + packagePath
                    + " need at least " + pkgSize + " left on device";
            throw new PortalException("Portal exception", "unzip package", "No space left on device.",
                    standardExceptionMessage);
        }
    }
}
