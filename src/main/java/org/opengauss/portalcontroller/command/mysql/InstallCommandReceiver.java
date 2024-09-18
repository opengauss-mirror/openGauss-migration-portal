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

package org.opengauss.portalcontroller.command.mysql;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.command.CommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.tools.common.MqTool;
import org.opengauss.portalcontroller.utils.CommandUtils;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.KafkaUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Install command receiver.
 */
public class InstallCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallCommandReceiver.class);

    public void action(String order) {
        if (InstallMigrationUtils.checkSudoPermission()) {
            InstallMigrationUtils.installDependencies("portal");
        }
        if (CommandUtils.containString(order, Command.ALL)) {
            InstallMigrationUtils.runAllInstallOrder(order);
        } else {
            InstallMigrationUtils.runInstallOrder(order);
        }
        ProcessUtils.sleepThread(1000, "unzip package");
        if (!CommandUtils.containString(order, Command.FULL)) {
            KafkaUtils.prepareConfluent();
            MqTool.getInstance().start(PortalControl.workspaceId);
        }
    }
}
