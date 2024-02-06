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
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.RunningTaskThread;

import java.util.ArrayList;
import java.util.List;

public class PlanTest {
    @Test
    public void runningThreadListTest() {
        List<RunningTaskThread> list = new ArrayList<>();
        RunningTaskThread runningTaskThread = new RunningTaskThread("test", "testProcess");
        list.add(runningTaskThread);
        Plan.setRunningTaskThreadsList(list);
        assert Plan.getRunningTaskThreadsList().contains(runningTaskThread);
        list.clear();
        Plan.setRunningTaskThreadsList(list);
    }
}
