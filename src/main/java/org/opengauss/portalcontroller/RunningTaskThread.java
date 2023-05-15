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

/**
 * Thread running task.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class RunningTaskThread {

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
     * Init a instance of RunningTaskThread with parameter methodname,processname,pid and tasklist.
     *
     * @param methodName  the method name
     * @param processName the process name
     * @param pid         the pid
     */
    public RunningTaskThread(String methodName, String processName, int pid) {
        this.methodName = methodName;
        this.processName = processName;
        this.pid = pid;
    }

    /**
     * Instantiates a new Running task thread.
     *
     * @param methodName  the method name
     * @param processName the process name
     * @param logPath     the log path
     */
    public RunningTaskThread(String methodName, String processName, String logPath) {
        this.methodName = methodName;
        this.processName = processName;
        this.logPath = logPath;
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
     *
     * @return the long
     */
    public void startTask() {
        PortalControl.MethodRunner methodRunner = Task.runTaskHandlerHashMap.get(methodName);
        methodRunner.runMethod(methodName);
    }

    /**
     * Stop task.Execute stop task command.
     */
    public void stopTask() {
        String stopMethodName = methodName.replaceFirst("run", "stop");
        PortalControl.EventHandler eventHandler = Task.stopTaskHandlerHashMap.get(stopMethodName);
        eventHandler.handle(stopMethodName);
    }
}