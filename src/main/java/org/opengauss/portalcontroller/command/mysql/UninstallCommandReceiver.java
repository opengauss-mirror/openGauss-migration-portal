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

import org.opengauss.portalcontroller.command.CommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.tools.common.MqTool;
import org.opengauss.portalcontroller.utils.CommandUtils;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;

/**
 * The type Uninstall command receiver.
 */
public class UninstallCommandReceiver extends CommandReceiver {
    public void action(String order) {
        InstallMigrationUtils installMigrationUtils = new InstallMigrationUtils();
        if (!CommandUtils.containString(order, Command.FULL)) {
            MqTool.getInstance().uninstall();
        }
        if (CommandUtils.containString(order, Command.ALL)) {
            installMigrationUtils.uninstallAllMigrationTools();
        } else {
            installMigrationUtils.uninstallMigrationTools(order);
        }
    }
}
