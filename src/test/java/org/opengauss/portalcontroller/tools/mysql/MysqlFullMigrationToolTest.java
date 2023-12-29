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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.RuntimeExecUtils;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MysqlFullMigrationToolTest {
    @Mock
    private MysqlFullMigrationTool mysqlFullMigrationToolUnderTest;
    private MockedStatic<RuntimeExecUtils> runtimeExecToolsMockedStatic;
    private MockedStatic<FileUtils> fileUtilsMockedStatic;
    private MockedStatic<LogViewUtils> logViewMockedStatic;
    private MockedStatic<PathUtils> pathUtilsMockedStatic;

    @BeforeAll
    void setUp() {
        fileUtilsMockedStatic.when(() -> FileUtils.createFile(Mockito.anyString(), Mockito.anyBoolean()))
                .thenAnswer(invocationOnMock -> null);
        fileUtilsMockedStatic.when(() -> FileUtils.outputFileString(Mockito.anyString())).thenReturn("");
        runtimeExecToolsMockedStatic = Mockito.mockStatic(RuntimeExecUtils.class);
        runtimeExecToolsMockedStatic.when(() -> RuntimeExecUtils.download(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(invocationOnMock -> null);
        runtimeExecToolsMockedStatic.when(() -> RuntimeExecUtils.runShell(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(invocationOnMock -> null);
        runtimeExecToolsMockedStatic.when(() -> RuntimeExecUtils.removeFile(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(invocationOnMock -> null);
        runtimeExecToolsMockedStatic.when(() -> RuntimeExecUtils.unzipFile(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString())).thenAnswer(invocationOnMock -> null);
        fileUtilsMockedStatic = Mockito.mockStatic(FileUtils.class);
        fileUtilsMockedStatic.when(() -> FileUtils.checkFileExist(Mockito.anyString(), Mockito.anyInt()))
                .thenAnswer(invocationOnMock -> null);
        logViewMockedStatic = Mockito.mockStatic(LogViewUtils.class);
        pathUtilsMockedStatic = Mockito.mockStatic(PathUtils.class);
        pathUtilsMockedStatic.when(() -> PathUtils.combainPath(Mockito.anyBoolean(), Mockito.any())).thenReturn("");
    }

    @AfterAll
    void after() {
        fileUtilsMockedStatic.close();
        runtimeExecToolsMockedStatic.close();
        fileUtilsMockedStatic.close();
        pathUtilsMockedStatic.close();
        logViewMockedStatic.close();
    }

    @Test
    void testInstallAllPackages() throws Exception {
        Mockito.doCallRealMethod().when(mysqlFullMigrationToolUnderTest).install(Mockito.anyBoolean());
        Mockito.doReturn(false).when(mysqlFullMigrationToolUnderTest)
                .checkChameleonStatus(Mockito.anyString(), Mockito.anyString());
        Mockito.doNothing().when(mysqlFullMigrationToolUnderTest).checkChameleonVersion(Mockito.anyString(),
                Mockito.anyString());
        mysqlFullMigrationToolUnderTest.install(false);
        Mockito.verify(mysqlFullMigrationToolUnderTest, Mockito.times(1))
                .checkChameleonVersion(Mockito.anyString(),
                        Mockito.anyString());
    }

    @Test
    void testCheckChameleonVersion() throws Exception {
        Mockito.doCallRealMethod().when(mysqlFullMigrationToolUnderTest).checkChameleonVersion("order",
                "chameleonInstallLogPath");

        runtimeExecToolsMockedStatic.when(() -> RuntimeExecUtils.executeOrder(Mockito.anyString(), Mockito.anyInt(),
                        Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(), Mockito.any()))
                .thenAnswer(invocationOnMock -> null);
        logViewMockedStatic.when(() -> LogViewUtils.getFullLog(Mockito.anyString())).thenReturn("chameleon");
        mysqlFullMigrationToolUnderTest.checkChameleonVersion("order", "chameleonInstallLogPath");
        runtimeExecToolsMockedStatic.verify(() -> RuntimeExecUtils.removeFile(Mockito.anyString(), Mockito.anyString()),
                Mockito.times(1));
    }

    @Test
    void testCheckChameleonStatus() throws PortalException {
        Mockito.doCallRealMethod().when(mysqlFullMigrationToolUnderTest).checkChameleonStatus(Mockito.anyString(),
                Mockito.anyString());
        Mockito.doNothing().when(mysqlFullMigrationToolUnderTest).checkChameleonVersion(Mockito.anyString(),
                Mockito.anyString());
        boolean b = mysqlFullMigrationToolUnderTest.checkChameleonStatus(Mockito.anyString(),
                Mockito.anyString());
        Assertions.assertEquals(b, true);
    }
}
