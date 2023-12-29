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

package org.opengauss.portalcontroller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.portalcontroller.task.WorkspacePath;

class WorkspacePathTest {
    private WorkspacePath workspacePathUnderTest;

    @BeforeEach
    void setUp() {
        workspacePathUnderTest = WorkspacePath.getInstance("root", "workspaceid");
    }

    @Test
    public void testGetInstance() {
        WorkspacePath workspacePathUnderTest1 = WorkspacePath.getInstance("root", "workspaceid");
        assert workspacePathUnderTest1 == workspacePathUnderTest;
    }
}
