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
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.command.CommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.tools.common.MqTool;
import org.opengauss.portalcontroller.utils.CommandUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Start command receiver.
 */
public class StartCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartCommandReceiver.class);

    public void action(String order) {
        if (order.equals(Command.Start.KAFKA)) {
            MqTool mqTool = MqTool.getInstance();
            if (mqTool != null) {
                mqTool.start("");
            }
        } else {
            PortalControl.startPlan(generateTaskList(order));
        }
    }

    /**
     * Generate task list list.
     *
     * @param order the order
     * @return the list
     */
    public List<String> generateTaskList(String order) {
        if (order.equals(Command.Start.Plan.CURRENT)) {
            String path = PathUtils.combainPath(true,
                    PortalControl.portalControlPath + "config", "currentPlan");
            return PortalControl.initTasklist(path);
        }
        if (CommandUtils.containString(order, Command.MYSQL)) {
            return new ArrayList<>() {{
                add(order);
            }};
        }
        String plan = order.replaceFirst(Command.Type.START, "").trim();
        if (PortalControl.planList.containsKey(plan)) {
            return PortalControl.planList.get(plan);
        } else {
            LOGGER.error("{}Invalid command.", ErrorCode.INVALID_COMMAND);
            return new ArrayList<>();
        }
    }
}
