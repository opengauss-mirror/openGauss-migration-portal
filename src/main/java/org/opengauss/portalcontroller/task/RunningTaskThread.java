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

package org.opengauss.portalcontroller.task;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Thread running task.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class RunningTaskThread {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RunningTaskThread.class);
    private String name;
    private String methodName;
    private String processName;
    private long pid;
    private String logPath;

    /**
     * Init a instance of RunningTaskThread.
     */
    public RunningTaskThread() {
        this.methodName = "";
        this.processName = "";
        pid = -1;
    }

    /**
     * Instantiates a new Running task thread.
     *
     * @param name the name
     */
    public RunningTaskThread(String name) {
        this.name = name;
        String getMethodName = Task.getMethodNameMap().get(name);
        this.methodName = getMethodName;
        String getProcessName = Task.getTaskProcessMap().get(getMethodName);
        this.processName = getProcessName;
        this.logPath = Task.getTaskLogMap().get(getMethodName);
        this.pid = ProcessUtils.getCommandPid(getProcessName);
    }

    /**
     * Init a instance of RunningTaskThread with parameter methodname and processname.
     *
     * @param methodName  The method name.
     * @param processName The process name.
     */
    public RunningTaskThread(String methodName, String processName) {
        this.methodName = methodName;
        this.processName = processName;
        pid = -1;
    }

    /**
     * Gets name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get pid.
     *
     * @return the pid
     */
    public long getPid() {
        return pid;
    }

    /**
     * Set pid.
     *
     * @param pid pid
     */
    public void setPid(long pid) {
        this.pid = pid;
    }

    /**
     * Get method name.
     *
     * @return methodName method name
     */
    public String getMethodName() {
        return methodName;
    }

    /**
     * Set method name.
     *
     * @param methodName methodName
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * Get process name.
     *
     * @return String processName
     */
    public String getProcessName() {
        return processName;
    }

    /**
     * Set process name.
     *
     * @param processName Process name.
     */
    public void setProcessName(String processName) {
        this.processName = processName;
    }

    /**
     * Gets log path.
     *
     * @return the log path
     */
    public String getLogPath() {
        return logPath;
    }

    /**
     * Sets log path.
     *
     * @param logPath the log path
     */
    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    /**
     * Start task.Execute start task command.
     */
    public void startTask() {
        PortalControl.MethodRunner methodRunner = Task.runTaskHandlerHashMap.get(methodName);
        methodRunner.runMethod(methodName);
    }

    /**
     * Stop task.Execute stop task command.
     *
     * @param order the order
     */
    public void stopTask(String order) {
        if (pid == -1) {
            LOGGER.info("No process {} to stop.", processName);
            return;
        }
        try {
            if (order.equals("")) {
                killProcess();
            } else {
                killProcessByOrder(order, PortalControl.toolsConfigParametersTable.get(Parameter.ERROR_PATH));
            }
        } catch (PortalException e) {
            e.setRequestInformation("Stop " + name + " failed.");
            LOGGER.error("{}{}", ErrorCode.COMMAND_EXECUTION_FAILED, e.toString());
            PortalControl.shutDownPortal(e.toString());
        }
        LOGGER.info("Stop {}.", name);
    }

    /**
     * Kill process.
     *
     * @param order     the order
     * @param errorPath the error path
     * @throws PortalException the portal exception
     */
    public void killProcessByOrder(String order, String errorPath) throws PortalException {
        String[] orderParts = order.split(" ");
        String executeFilePath = orderParts[0];
        if (!new File(executeFilePath).exists()) {
            LOGGER.error("No file " + executeFilePath + " to execute.");
            String killOrder = "kill -9 " + pid;
            RuntimeExecUtils.executeOrder(killOrder, 3000, errorPath);
        } else {
            RuntimeExecUtils.executeOrder(order, 3000, errorPath);
        }
    }

    private void killProcess() throws PortalException {
        int processStopTime = 5000;
        ProcessUtils.killProcessByCommandSnippet(processName, processStopTime, false);

        int oneSecond = 1000;
        while (processStopTime > 0) {
            try {
                Thread.sleep(oneSecond);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for process to stop", e);
            }

            processStopTime -= oneSecond;
            if (ProcessUtils.getCommandPid(processName) == -1) {
                LOGGER.info("{} stopped", processName);
                return;
            }
        }

        ProcessUtils.killProcessByCommandSnippet(processName, processStopTime, true);
        pid = ProcessUtils.getCommandPid(processName);
        if (pid == -1) {
            LOGGER.info("{} stopped", processName);
        } else {
            LOGGER.error("Failed to stop {}, please kill it manually, pid: {}", processName, pid);
        }
    }
}