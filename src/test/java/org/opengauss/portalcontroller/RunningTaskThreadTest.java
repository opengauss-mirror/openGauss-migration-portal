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

import org.junit.jupiter.api.Test;
import org.opengauss.portalcontroller.task.RunningTaskThread;

public class RunningTaskThreadTest {
    @Test
    public void test() {
        RunningTaskThread runningTaskThread = new RunningTaskThread("testThread", "testProcess");
        runningTaskThread.setMethodName("testMethod");
        runningTaskThread.setProcessName("testProcess1");
        assert runningTaskThread.getMethodName().equals("testMethod");
        assert runningTaskThread.getProcessName().equals("testProcess1");
    }
}
