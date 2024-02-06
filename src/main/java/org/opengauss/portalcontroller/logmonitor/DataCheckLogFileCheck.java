/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.logmonitor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.opengauss.portalcontroller.constant.Check.CheckLog.DATA_CHECK_START_INFO_LIST;
import static org.opengauss.portalcontroller.constant.Check.CheckLog.DATA_CHECK_STOP_INFO_LIST;

/**
 * DataCheckLogFileCheck
 *
 * @author: www
 * @date: 2023/11/15 10:10
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
@Getter
@Slf4j
public class DataCheckLogFileCheck {
    @Getter
    @Setter
    private static boolean isDataCheckFinish = false;

    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4,
            5,
            8,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(6),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());

    private LogFileListener sourceLogListener;

    private LogFileListener sinkLogListener;

    private LogFileListener appLogListener;

    private LogFileListener checkResultListener;

    /**
     * startCheck
     *
     * @author: www
     * @date: 2023/11/16 10:09
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public void startCheck() {
        String checkSourceLogPath = PortalControl.toolsConfigParametersTable.get(Check.Source.LOG_PATH);
        String checkSinkLogPath = PortalControl.toolsConfigParametersTable.get(Check.Sink.LOG_PATH);
        String checkLogPath = PortalControl.toolsConfigParametersTable.get(Check.LOG_PATH);
        String checkResultFile = PortalControl.toolsConfigParametersTable.get(Check.Result.FULL_CURRENT)
                + "process.pid";
        List<String> checkLogStrs = List.of(Check.CheckLog.EXCEPTION,
                Check.CheckLog.ERR, Check.CheckLog.ERR_UPPER);
        sinkLogListener = new LogFileListener(checkSinkLogPath, checkLogStrs);
        sourceLogListener = new LogFileListener(checkSourceLogPath, checkLogStrs);
        appLogListener = new LogFileListener(checkLogPath, checkLogStrs);
        List<String> checkList = new ArrayList<>();
        checkList.addAll(DATA_CHECK_START_INFO_LIST);
        checkList.addAll(DATA_CHECK_STOP_INFO_LIST);
        checkResultListener = new LogFileListener(checkResultFile, checkList);
        threadPool.allowCoreThreadTimeOut(true);
        threadPool.execute(sinkLogListener);
        threadPool.execute(sourceLogListener);
        threadPool.execute(appLogListener);
        threadPool.execute(checkResultListener);
    }

    /**
     * getErrResult
     *
     * @return boolean
     * @author: www
     * @date: 2023/11/16 10:10
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public boolean getErrResult() {
        boolean isSinkCheckErr = sinkLogListener.getLogMap().containsKey(Check.CheckLog.EXCEPTION)
                || sinkLogListener.getLogMap().containsKey(Check.CheckLog.ERR)
                || sinkLogListener.getLogMap().containsKey(Check.CheckLog.ERR_UPPER);
        boolean isSourceCheckErr = sourceLogListener.getLogMap().containsKey(Check.CheckLog.EXCEPTION)
                || sourceLogListener.getLogMap().containsKey(Check.CheckLog.ERR)
                || sourceLogListener.getLogMap().containsKey(Check.CheckLog.ERR_UPPER);
        boolean isAppErrCheck = appLogListener.getLogMap().containsKey(Check.CheckLog.EXCEPTION)
                || appLogListener.getLogMap().containsKey(Check.CheckLog.ERR)
                || appLogListener.getLogMap().containsKey(Check.CheckLog.ERR_UPPER);
        return !(isSinkCheckErr || isSourceCheckErr || isAppErrCheck);
    }

    /**
     * stopListener
     *
     * @author: www
     * @date: 2023/11/16 10:12
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public void stopListener() {
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            log.error("InterruptedException: ", e);
        }
        sinkLogListener.stop();
        sourceLogListener.stop();
        appLogListener.stop();
        checkResultListener.stop();
        threadPool.shutdownNow();
    }
}
