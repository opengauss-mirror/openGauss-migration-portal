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

package org.opengauss.portalcontroller.logmonitor.listener;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.opengauss.portalcontroller.logmonitor.DataCheckLogFileCheck;
import org.opengauss.portalcontroller.task.Plan;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opengauss.portalcontroller.constant.Check.CheckLog.DATA_CHECK_STOP_INFO_LIST;

/**
 * some msg
 *
 * @date :2023/11/14 11:20
 * @description: some description
 * @version: 1.1
 * @since 1.1
 */
@Getter
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class LogFileListener implements Runnable {
    @Getter
    private final HashMap<String, String> logMap = new HashMap<>();

    private Tailer tailer;

    private String filePath;

    private List<String> checkStrList;


    public LogFileListener(String filePath, List<String> checkStrList) {
        this.filePath = filePath;
        this.checkStrList = checkStrList;
    }

    @Override
    public void run() {
        while (!new File(filePath).exists()) {
            if (Plan.stopPlan) {
                return;
            }
            log.info("check file {}...", filePath);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                log.error("InterruptedException:", e);
                return;
            }
        }
        initLogFileListener();
    }

    /**
     * initLogFileListener
     *
     * @author: www
     * @date: 2023/11/16 10:02
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public void initLogFileListener() {
        tailer = Tailer.create(new File(filePath), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                if (Plan.stopPlan) {
                    stop();
                    return;
                }
                if (checkStrList.isEmpty()) {
                    return;
                }
                for (String checkStr : checkStrList) {
                    if (line.contains(checkStr)) {
                        log.error("{} find check str... {}....{}", filePath, line, checkStr);
                        logMap.put(checkStr, line);
                    }
                }
            }
        }, 2000);
        log.info("data check listener is started  check file={}.", filePath);
    }

    /**
     * stop
     *
     * @author: www
     * @date: 2023/11/16 10:03
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public void stop() {
        log.info("logmap is {}", logMap);
        if (logMap.keySet().containsAll(DATA_CHECK_STOP_INFO_LIST)) {
            log.info("change data check finish flag {}", DataCheckLogFileCheck.isDataCheckFinish());
            DataCheckLogFileCheck.setDataCheckFinish(true);
        }
        if (Objects.nonNull(tailer)) {
            tailer.stop();
            logMap.clear();
        }
    }
}
