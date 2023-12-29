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

import org.opengauss.portalcontroller.checkportalstatus.BaseCheckNode;
import org.opengauss.portalcontroller.checkportalstatus.ChameleonCheckNode;
import org.opengauss.portalcontroller.checkportalstatus.KafkaCheckStatusNode;
import org.opengauss.portalcontroller.checkportalstatus.NecessaryFileCheckStatusNode;
import org.opengauss.portalcontroller.command.CommandReceiver;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.utils.LogViewUtils;

import java.util.Arrays;
import java.util.List;

/**
 * CheckPortalStatusCommandReceiver
 *
 * @date :2023/9/19 16:22
 * @description: CheckPortalStatusCommandReceiver
 * @version: 1.1
 * @since 1.1
 */
public class CheckPortalStatusCommandReceiver extends CommandReceiver {
    /**
     * check portal status command action
     *
     * @param order the order
     */
    public void action(String order) {
        List<BaseCheckNode> checkNodes = Arrays.asList(new ChameleonCheckNode(),
                new KafkaCheckStatusNode(), new NecessaryFileCheckStatusNode());
        for (BaseCheckNode checkNode : checkNodes) {
            checkNode.checkStatus();
            if (checkNode.getStatus() != 0) {
                LogViewUtils.outputResult(false, checkNode.getMsg());
                return;
            }
        }
        LogViewUtils.outputResult(true, Check.CheckPortalStatus.CHECK_STATUS_PREFIX
                + Check.CheckPortalStatus.CHECK_STATUS_OK);
    }
}
