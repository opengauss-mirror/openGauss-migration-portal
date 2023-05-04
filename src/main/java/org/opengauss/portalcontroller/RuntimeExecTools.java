package org.opengauss.portalcontroller;

import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;


/**
 * The type Runtime exec tools.
 */
public class RuntimeExecTools {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RuntimeExecTools.class);

    /**
     * Execute order long.
     *
     * @param command       the command
     * @param time          the time
     * @param errorFilePath the error file path
     * @return the long
     * @throws PortalException the portal exception
     */
    public static long executeOrder(String command, int time, String errorFilePath) throws PortalException {
        long pid = -1;
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
            Tools.writeFile(errorStr, new File(errorFilePath), true);
        } catch (IOException e) {
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        } catch (PortalException e) {
            throw e;
        }
        return pid;
    }

    /**
     * Execute order.
     *
     * @param command            the command
     * @param time               the time
     * @param workDirectory      the work directory
     * @param errorFilePath      the error file path
     * @param shouldChangeOutput the should change output
     * @throws PortalException the portal exception
     */
    public static void executeOrder(String command, int time, String workDirectory, String errorFilePath,
                                    boolean shouldChangeOutput) throws PortalException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String[] commands = command.split(" ");
        processBuilder.directory(new File(workDirectory));
        processBuilder.command(commands);
        if (shouldChangeOutput) {
            processBuilder.redirectErrorStream(true);
            processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(errorFilePath)));
        } else {
            processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File(errorFilePath)));
        }
        try {
            Process process = processBuilder.start();
            String errorStr = "";
            if (time == 0) {
                int retCode = process.waitFor();
                if (retCode == 0) {
                    LOGGER.info("Execute order finished.");
                } else {
                    errorStr = getInputStreamString(process.getErrorStream());
                    if (!errorStr.equals("")) {
                        LOGGER.error(errorStr);
                    }
                }
            } else {
                process.waitFor(time, TimeUnit.MILLISECONDS);
            }
        } catch (IOException e) {
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        } catch (PortalException e) {
            throw e;
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
    public static void executeOrderCurrentRuntime(String[] cmdParts, int time, String outputFilePath, String errorLog) throws PortalException {
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
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFilePath, true));
                if (str != null && !str.equals("")) {
                    bufferedWriter.write(str);
                } else {
                    LOGGER.error(errorLog);
                }
                bufferedWriter.flush();
                bufferedWriter.write(errorStr);
                bufferedWriter.flush();
                bufferedWriter.close();
            }
        } catch (IOException e) {
            String command = Tools.combainOrder(cmdParts);
            throw new PortalException("IO exception", "executing command " + command, e.getMessage());
        } catch (InterruptedException e) {
            String command = Tools.combainOrder(cmdParts);
            throw new PortalException("Interrupted exception", "executing command " + command, e.getMessage());
        } catch (PortalException e) {
            throw e;
        }
    }

    /**
     * Download boolean.
     *
     * @param urlParameter  the url parameter
     * @param pathParameter the path parameter
     * @return the boolean
     * @throws PortalException the portal exception
     */
    public static boolean download(String urlParameter, String pathParameter) throws PortalException {
        boolean flag = true;
        String url = PortalControl.toolsConfigParametersTable.get(urlParameter);
        String path = PortalControl.toolsConfigParametersTable.get(pathParameter);
        String[] urlParameters = url.split(File.separator);
        String packageName = urlParameters[urlParameters.length - 1];
        try {
            Tools.createFile(path, false);
            File file = new File(path + packageName);
            if (file.exists() && file.isFile()) {
                LOGGER.info("File " + path + packageName + " already exists.Skip the download package.");
                flag = false;
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
        return flag;
    }

    /**
     * Gets input stream string.
     *
     * @param in the in
     * @return the input stream string
     */
    public static String getInputStreamString(InputStream in) throws PortalException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String str = "";
        StringBuilder sb = new StringBuilder();
        try {
            while ((str = br.readLine()) != null) {
                sb.append(str + System.lineSeparator());
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
            boolean exist = new File(newFilePath).exists();
            if (!exist || recovery) {
                String command = "cp -R " + filePath + " " + directory;
                try {
                    executeOrder(command, 60000, PortalControl.portalErrorPath);
                } catch (PortalException e) {
                    throw e;
                }
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
            try {
                String command = "rm -rf " + path;
                executeOrder(command, 60000, errorFilePath);
                LOGGER.info("Remove file " + path + " finished.");
            } catch (PortalException e) {
                throw e;
            }
        } else {
            LOGGER.info("No file " + path + " to remove.");
        }
    }

    /**
     * Unzip file.
     *
     * @param packagePath the package path
     * @param directory   the directory
     * @throws PortalException the portal exception
     */
    public static void unzipFile(String packagePath, String directory) throws PortalException {
        String command = "";
        if (!new File(packagePath).exists()) {
            LOGGER.error("Error message: No package to install.");
        }
        try {
            if (packagePath.endsWith(".zip")) {
                command = "unzip -q -o " + packagePath + " -d " + directory;
                executeOrder(command, 900000, PortalControl.portalErrorPath);
                LOGGER.info("Unzip file finished.");
            } else if (packagePath.endsWith(".tar.gz") || packagePath.endsWith(".tgz")) {
                command = "tar -zxf " + packagePath + " -C " + directory;
                executeOrder(command, 900000, PortalControl.portalErrorPath);
                LOGGER.info("Unzip file " + packagePath + " to " + directory + " finished.");
            } else {
                throw new PortalException("", "unzip package", "Invalid package type", "Invalid package type.Please check if the package is ends with .zip or .tar.gz or .tgz");
            }
        } catch (PortalException e) {
            e.setRequestInformation("Unzip package failed.");
            throw e;
        }
    }

    /**
     * Rename.
     *
     * @param oldName the old name
     * @param newName the new name
     * @throws PortalException the portal exception
     */
    public static void rename(String oldName, String newName) throws PortalException {
        try {
            String command = "mv " + oldName + " " + newName;
            if (new File(oldName).exists()) {
                executeOrder(command, 600000, PortalControl.portalErrorPath);
            }
            LOGGER.info("Rename file " + oldName + " to " + newName + " finished.");
        } catch (PortalException e) {
            throw e;
        }
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
    public static void copyFileStartWithWord(File file, String workDirectory, String criticalWord, String replaceWord, boolean recovery) throws PortalException {
        try {
            if (file.getName().startsWith(criticalWord)) {
                RuntimeExecTools.copyFile(file.getAbsolutePath(), workDirectory + replaceWord, recovery);
            }
        } catch (PortalException e) {
            throw e;
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
        try {
            if (!workDirectory.equals("")) {
                RuntimeExecTools.executeOrder(command, time, workDirectory, errorFilePath, shouldChangeOutput);
            } else {
                RuntimeExecTools.executeOrder(command, time, errorFilePath);
            }
            LOGGER.info(information + ".");
        } catch (PortalException e) {
            e.setRequestInformation(information + " failed");
            e.shutDownPortal(LOGGER);
        }
    }
}
