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

package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Show command receiver.
 */
public class ShowCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowCommandReceiver.class);

    public void action(String order) {
        switch (order) {
            case Command.Show.PLAN:
                PortalControl.showPlanList();
                break;
            case Command.Show.STATUS:
                PortalControl.showStatus();
                break;
            case Command.Show.INFORMATION:
                PortalControl.showMigrationParameters();
                break;
            case Command.Show.PARAMETERS:
                PortalControl.showParameters();
                break;
            default:
                LOGGER.error("Invalid command.");
                break;
        }
    }
}
