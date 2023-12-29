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
import org.opengauss.portalcontroller.task.Task;

import java.util.ArrayList;
import java.util.HashMap;

public class TaskTest {
    @Test
    public void checkPlanTest() {
        ArrayList<String> list1 = new ArrayList<>();
        list1.add("start mysql full migration");
        list1.add("start mysql full migration datacheck");
        assert Task.checkPlan(list1);
        ArrayList<String> list2 = new ArrayList<>();
        list2.add("start mysql full migration");
        list2.add("start mysql full migration");
        assert !Task.checkPlan(list2);
        ArrayList<String> list3 = new ArrayList<>();
        list3.add("start mysql incremental migration");
        list3.add("start mysql full migration");
        assert !Task.checkPlan(list3);
        ArrayList<String> list4 = new ArrayList<>();
        list4.add("start mysql incremental migration");
        list4.add("start mysql full migration datacheck");
        assert !Task.checkPlan(list4);
        ArrayList<String> list5 = new ArrayList<>();
        list5.add("test1");
        list5.add("test2");
        assert !Task.checkPlan(list5);
    }

    @Test
    public void taskProcessMapTest() {
        Task.initTaskProcessMap();
        assert Task.getTaskProcessMap().containsKey("runKafka");
        HashMap<String, String> map = new HashMap<>();
        map.put("test", "testProcess");
        Task.setTaskProcessMap(map);
        assert Task.getTaskProcessMap().get("test").equals("testProcess");
        map.clear();
        Task.setTaskProcessMap(map);
    }
}
