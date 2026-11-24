/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.handler;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.AlertLogCollectionManager;
import org.opengauss.portalcontroller.alert.AlertLogEntity;
import org.opengauss.portalcontroller.alert.AlertLogFileUtils;
import org.opengauss.portalcontroller.task.Plan;

/**
 * uncaught thread exception handler
 *
 * @since 2024/12/20
 */
@Slf4j
public class ThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        String errorMessage = String.format("thread %s occur exception: ", t.getName());
        log.error(errorMessage, e);
        handleException(errorMessage, t, e);
    }

    private void handleException(String errorMessage, Thread t, Throwable e) {
        AlertLogFileUtils.printUncaughtError(
                AlertLogEntity.builder()
                        .className(ThreadExceptionHandler.class.getName())
                        .thread(t.getName())
                        .message(errorMessage)
                        .build(), e);

        PortalControl.shutDownPortal(e.getMessage());
        Plan.getInstance(PortalControl.workspaceId).stopPlan(true);
        PortalControl.threadStatusController.setExit(true);
        PortalControl.threadGetOrder.exit = true;
        AlertLogCollectionManager.stopCollection();
    }
}
