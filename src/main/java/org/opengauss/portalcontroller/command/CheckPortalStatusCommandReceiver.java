/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.checkportalstatus.BaseCheckNode;
import org.opengauss.portalcontroller.checkportalstatus.ChameleonCheckNode;
import org.opengauss.portalcontroller.checkportalstatus.KafkaCheckStatusNode;
import org.opengauss.portalcontroller.checkportalstatus.NecessaryFileCheckStatusNode;
import org.opengauss.portalcontroller.constant.Check;

import java.util.Arrays;
import java.util.List;

/**
 * CheckPortalStatusCommandReceiver
 *
 * @since 1.1
 * @date :2023/9/19 16:22
 * @description: CheckPortalStatusCommandReceiver
 * @version: 1.1
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
                Tools.outputResult(false, checkNode.getMsg());
                return;
            }
        }
        Tools.outputResult(true, Check.CheckPortalStatus.CHECK_STATUS_PREFIX + Check.CheckPortalStatus.CHECK_STATUS_OK);
    }
}
