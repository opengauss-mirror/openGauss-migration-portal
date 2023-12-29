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

package org.opengauss.portalcontroller.tools.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.logmonitor.DataCheckLogFileCheck;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.task.Task;

import java.lang.reflect.Field;

@ExtendWith(MockitoExtension.class)
class FullDatacheckToolTest {
    @Mock
    private FullDatacheckTool fullDatacheckToolUnderTest;

    @Mock
    DataCheckLogFileCheck fileCheck;

    @Test
    void testPrepareWork() {
        Mockito.doCallRealMethod().when(fullDatacheckToolUnderTest).init(Mockito.anyString());
        fullDatacheckToolUnderTest.init("1");
    }

    @Test
    void testStart() {
        try {
            Field fileCheckFiled = FullDatacheckTool.class.getDeclaredField("fileCheck");
            fileCheckFiled.setAccessible(true);
            fileCheckFiled.set(fullDatacheckToolUnderTest, fileCheck);
            Mockito.doCallRealMethod().when(fullDatacheckToolUnderTest).start(Mockito.anyString());
            Mockito.doNothing().when(fullDatacheckToolUnderTest).stop();
            try (MockedStatic<Task> taskMocked = Mockito.mockStatic(Task.class)) {
                taskMocked.when(() -> Task.startDataCheck(Mockito.any(LogFileListener.class)))
                        .then(invocationOnMock -> null);
                PortalControl.status = Status.ERROR;
                fullDatacheckToolUnderTest.start("1");
                Assertions.assertEquals(PortalControl.status, Status.ERROR);
                PortalControl.status = 111;
                fullDatacheckToolUnderTest.start("1");
                Assertions.assertEquals(PortalControl.status, Status.RUNNING_FULL_MIGRATION_CHECK);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
