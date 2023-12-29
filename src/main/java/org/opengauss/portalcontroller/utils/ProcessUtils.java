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
 * some msg
 *
 * @date :2024/1/16 16:34
 * @description: some description
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
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "search process",
                    e.getMessage());
            LOGGER.error(portalException.toString());
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
                        LOGGER.error(e.toString());
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
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "closing chameleon "
                    + "process", e.getMessage());
            portalException.setRequestInformation("Close full migration tools failed");
            LOGGER.error(portalException.toString());
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
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        } catch (InterruptedException e) {
            PortalException portalException = new PortalException("Interrupted exception", "checking whether another "
                    + "portal is running", e.getMessage());
            portalException.setRequestInformation("Checking whether another portal is running failed.Some tools "
                    + "cannot be closed");
            LOGGER.error(portalException.toString());
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
        String processName = Task.getTaskProcessMap().get(methodName);
        String errorStr =
                "Error message: Process " + processName + " exit abnormally or process " + System.lineSeparator();
        String logPath = Task.getTaskLogMap().get(methodName);
        errorStr += LogViewUtils.getErrorMsg(logPath) + System.lineSeparator();
        errorStr += "Please read " + logPath + " or error.log to get information.";
        PortalControl.status = Status.ERROR;
        PortalControl.errorMsg = errorStr;
        LOGGER.error(errorStr);
    }
}
