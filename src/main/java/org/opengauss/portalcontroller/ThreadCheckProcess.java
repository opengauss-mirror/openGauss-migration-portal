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

import org.opengauss.portalcontroller.constant.LogParseConstants;

/**
 * Thread check the process.
 *
 * @author ：liutong
 * @date ：Created in 2022/12/24
 * @since ：1
 */
public class ThreadCheckProcess extends Thread implements Runnable {
    /**
     * The Exit.
     */
    public boolean exit = false;

    /**
     * Run.Check if migration tools are running.
     */
    @Override
    public void run() {
        while (!exit && !Plan.stopPlan && Plan.checkRunningThreads()) {
            Tools.sleepThread(LogParseConstants.PERIOD_WATCH_LOG, "checking process");
        }
    }
}
