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

package org.opengauss.portalcontroller.checkportalstatus;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.tools.common.MqTool;

/**
 * KafkaCheckStatusNode
 *
 * @date :2023/9/19 16:22
 * @description: KafkaCheckStatusNode
 * @version: 1.1
 * @since 1.1
 */
public class KafkaCheckStatusNode extends BaseCheckNode {
    @Override
    public void checkStatus() {
        if (!MqTool.getInstance().checkStatus(PortalControl.workspaceId)) {
            this.setStatus(1);
            this.setMsg(Check.CheckPortalStatus.CHECK_STATUS_PREFIX + Check.CheckPortalStatus.CHECK_STATUS_KAFKA_ERR);
        }
    }
}
