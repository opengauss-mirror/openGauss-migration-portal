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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.portalcontroller.utils.ProcessUtils;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReverseMigrationToolTest {
    @Mock
    private ReverseMigrationTool reverseMigrationToolUnderTest;

    @BeforeEach
    void setUp() {
    }

    @Test
    void testCheckAnotherConnectExists() {
        Mockito.doCallRealMethod().when(reverseMigrationToolUnderTest).checkAnotherConnectExists();
        try (MockedStatic<ProcessUtils> toolsMocked = Mockito.mockStatic(ProcessUtils.class)) {
            toolsMocked.when(() -> ProcessUtils.getCommandPid(Mockito.any())).thenReturn(1);
            assertThat(reverseMigrationToolUnderTest.checkAnotherConnectExists()).isTrue();
            toolsMocked.when(() -> ProcessUtils.getCommandPid(Mockito.any())).thenReturn(-1);
            assertThat(reverseMigrationToolUnderTest.checkAnotherConnectExists()).isFalse();
        }
    }
}
