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
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.RunningTaskThread;
import org.opengauss.portalcontroller.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;

/**
 * ProcessUtils
 *
 * @date :2024/1/16 16:34
 * @description: ProcessUtils
 * @version: 1.1
 * @since 1.1
 */
public class ProcessUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessUtils.class);

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
            LOGGER.error("{}Failed to execute 'ps ux' to search process", ErrorCode.IO_EXCEPTION, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "search process",
                    e.getMessage());
            LOGGER.error("{}Failed to execute 'ps ux' to search process", ErrorCode.COMMAND_EXECUTION_FAILED, e);
            PortalControl.shutDownPortal(portalException.toString());
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
     * Gets command pid need retry
     *
     * @param processName the process name
     * @return the command pid
     */
    public static int getCommandPidNeedRetry(String processName) {
        for (int i = 0; i < 6; i++) {
            int commandPid = getCommandPid(processName);
            if (commandPid != -1) {
                return commandPid;
            }

            try {
                if (i < 5) {
                    LOGGER.warn("Can not find process '{}', try again after 1s", processName);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Thread sleep interrupted, when get command pid, error: {}", e.getMessage());
            }
        }
        return -1;
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
                pid = getCommandPid(runningTaskThread.getProcessName());
            }
        }
        return pid;
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
                        RuntimeExecUtils.executeOrder("kill -9 " + pid, 20, PortalControl.portalErrorPath);
                    } catch (PortalException e) {
                        e.setRequestInformation("Close chameleon failed");
                        LOGGER.error("{}Failed to kill process, pid: {}", ErrorCode.COMMAND_EXECUTION_FAILED, pid, e);
                        PortalControl.shutDownPortal(e.toString());
                    }
                }
            }
            br.close();
            in.close();
            pro.waitFor();
            pro.destroy();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "closing chameleon process",
                    e.getMessage());
            portalException.setRequestInformation("Close full migration tools failed");
            LOGGER.error("{}Failed to kill process, process: {}", ErrorCode.IO_EXCEPTION, command, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "closing chameleon "
                    + "process", e.getMessage());
            portalException.setRequestInformation("Close full migration tools failed");
            LOGGER.error("{}Failed to kill process, process: {}", ErrorCode.COMMAND_EXECUTION_FAILED, command, e);
            PortalControl.shutDownPortal(portalException.toString());
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
            PortalException portalException = new PortalException("IO exception", "checking whether another portal is"
                    + " running", e.getMessage());
            portalException.setRequestInformation("Checking whether another portal is running failed.Some tools "
                    + "cannot be closed");
            LOGGER.error("{}Failed to execute 'ps ux' to search process", ErrorCode.IO_EXCEPTION, e);
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "checking whether another "
                    + "portal is running", e.getMessage());
            portalException.setRequestInformation("Checking whether another portal is running failed.Some tools "
                    + "cannot be closed");
            LOGGER.error("{}Failed to execute 'ps ux' to search process", ErrorCode.COMMAND_EXECUTION_FAILED, e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        return signal;
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
     * checkProcess
     *
     * @param methodName methodName
     */
    public static void checkProcess(String methodName) {
        LOGGER.info("methodName = {}", methodName);
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement ele : stackTrace) {
            LOGGER.info(ele.getClassName() + "." + ele.getMethodName());
        }

        String processName = Task.getTaskProcessMap().get(methodName);
        String errorStr =
                "Error message: Process " + processName + " exit abnormally." + System.lineSeparator();
        String logPath = Task.getTaskLogMap().get(methodName);
        errorStr += LogViewUtils.getErrorMsg(logPath) + System.lineSeparator();
        errorStr += "Please read " + logPath + " or error.log to get information.";
        PortalControl.status = Status.ERROR;
        PortalControl.errorMsg = errorStr;
        LOGGER.error(errorStr);
    }

    /**
     * check incremental and reverse process
     *
     * @param methodName method name
     */
    public static void checkIncProcess(String methodName) {
        checkIncRevProcess(methodName, Status.CONNECT_ERROR);
    }

    /**
     * check incremental and reverse process
     *
     * @param methodName method name
     */
    public static void checkRevProcess(String methodName) {
        checkIncRevProcess(methodName, Status.REVERSE_CONNECT_ERROR);
    }

    private static void checkIncRevProcess(String methodName, Integer errorCode) {
        LOGGER.info("check process methodName = {}", methodName);
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement ele : stackTrace) {
            LOGGER.info("{}{}{}", ele.getClassName(), ".", ele.getMethodName());
        }
        String processName = Task.getTaskProcessMap().get(methodName);
        String errorStr = "Error message: Process " + processName + " exit abnormally." + System.lineSeparator();
        String logPath = Task.getTaskLogMap().get(methodName);
        errorStr += LogViewUtils.getErrorMsg(logPath) + System.lineSeparator();
        errorStr += "Please read " + logPath + " or error.log to get information.";
        PortalControl.status = errorCode;
        PortalControl.errorMsg = errorStr;
        Plan.pause = true;
        LOGGER.info("checkIncProcess {} Plan.pause={} and PortalControl.status={}", methodName, Plan.pause,
            PortalControl.status);
        LOGGER.error("{}{}", ErrorCode.PROCESS_EXITS_ABNORMALLY, errorStr);
    }

    /**
     * Kill the processes with the given command snippet
     *
     * @param commandSnippet the command snippet
     * @param time the time
     * @param isForce if true, use kill -9 to kill the process, otherwise use kill -15 to kill the process
     * @throws PortalException the portal exception
     */
    public static void killProcessByCommandSnippet(String commandSnippet, int time, boolean isForce)
            throws PortalException {
        String killCommandPart = isForce ? "xargs -I {} kill -s KILL {}"
                : "xargs -I {} kill -s TERM {} || xargs -I {} kill -s KILL {}";
        String killCommand = String.format("ps -ef | grep -- '%s' | grep -v grep | awk '{print $2}' | %s",
                commandSnippet, killCommandPart);
        RuntimeExecUtils.executeOrderByBash(killCommand, time,
                PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH));
    }
}
