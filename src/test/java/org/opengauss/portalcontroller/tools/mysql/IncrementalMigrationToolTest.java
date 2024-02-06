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
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class IncrementalMigrationToolTest {
    @Mock
    private IncrementalMigrationTool incrementalMigrationToolMocked;

    @Test
    void testStart() {
        Mockito.doCallRealMethod().when(incrementalMigrationToolMocked).start(Mockito.anyString());
        Mockito.doReturn(true).when(incrementalMigrationToolMocked).stop();
        try (MockedStatic<ParamsUtils> ParamsUtilsMocked = Mockito.mockStatic(ParamsUtils.class);
             MockedStatic<KafkaUtils> KafkaUtilsMocked = Mockito.mockStatic(KafkaUtils.class);
             MockedStatic<PropertitesUtils> PropertitesUtilsMocked = Mockito.mockStatic(PropertitesUtils.class)) {
            KafkaUtilsMocked.when(() -> KafkaUtils.changekafkaLogParam(Mockito.anyString(), Mockito.anyString()))
                    .thenAnswer(invocationOnMock -> null);
            ParamsUtilsMocked.when(() -> ParamsUtils.getAvailablePorts(Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt()))
                    .thenReturn(new ArrayList<>(List.of(1)));
            PropertitesUtilsMocked.when(() -> PropertitesUtils.changeSinglePropertiesParameter(Mockito.anyString(),
                    Mockito.anyString(),
                    Mockito.anyString())).thenAnswer(invocationOnMock -> null);
            try (MockedStatic<Task> taskMocked = Mockito.mockStatic(Task.class)) {
                taskMocked.when(() -> Task.startTaskMethod(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(),
                        Mockito.any(LogFileListener.class))).thenAnswer(invocationOnMock -> null);
                PortalControl.status = Status.ERROR;
                incrementalMigrationToolMocked.start("1");
                Assertions.assertEquals(PortalControl.status, Status.ERROR);
                PortalControl.status = 100;
                incrementalMigrationToolMocked.start("1");
                Assertions.assertEquals(PortalControl.status, Status.RUNNING_INCREMENTAL_MIGRATION);
            }
        }
    }

    @Test
    void testCheckEnd() {
        Mockito.doCallRealMethod().when(incrementalMigrationToolMocked).stop();
        Plan.stopPlan = true;
        boolean stop = incrementalMigrationToolMocked.stop();
        Assertions.assertEquals(true, stop);
    }

    @Test
    void testCheckAnotherConnectExists() {
        Mockito.doCallRealMethod().when(incrementalMigrationToolMocked).checkAnotherConnectExists();
        try (MockedStatic<ProcessUtils> toolsMockedStatic = Mockito.mockStatic(ProcessUtils.class)) {
            toolsMockedStatic.when(() -> ProcessUtils.getCommandPid(Mockito.any())).thenReturn(-1);
            assertThat(incrementalMigrationToolMocked.checkAnotherConnectExists()).isFalse();
            toolsMockedStatic.when(() -> ProcessUtils.getCommandPid(Mockito.any())).thenReturn(1);
            assertThat(incrementalMigrationToolMocked.checkAnotherConnectExists()).isTrue();
        }
    }

    @Test
    void testUninstall() {
        Mockito.doCallRealMethod().when(incrementalMigrationToolMocked).uninstall();
        try (MockedStatic<InstallMigrationUtils> installMigrationToolsMockedStatic =
                     Mockito.mockStatic(InstallMigrationUtils.class)) {
            installMigrationToolsMockedStatic.when(() -> InstallMigrationUtils
                            .removeSingleMigrationToolFiles(Mockito.any(), Mockito.anyString()))
                    .thenAnswer(invocationOnMock -> null);
            incrementalMigrationToolMocked.uninstall();
            installMigrationToolsMockedStatic.verify(() -> InstallMigrationUtils
                            .removeSingleMigrationToolFiles(Mockito.any(), Mockito.anyString()),
                    Mockito.times(1));
        }
    }
}
