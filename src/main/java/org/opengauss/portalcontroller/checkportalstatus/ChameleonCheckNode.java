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

import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.check.CheckTaskMysqlFullMigration;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.exception.PortalException;

/**
 * ChameleonCheckNode
 *
 * @since 1.1
 * @date :2023/9/19 16:22
 * @description: ChameleonCheckNode
 * @version: 1.1
 */
public class ChameleonCheckNode extends BaseCheckNode {
    @Override
    public void checkStatus() {
        checkChameleonStatus();
    }

    /**
     * check portal status chameleon
     */
    public void checkChameleonStatus() {
        String chameleonVersionOrder =
                PortalControl.toolsConfigParametersTable.get(Chameleon.RUNNABLE_FILE_PATH) + " --version";
        String chameleonInstallLogPath = PathUtils.combainPath(true, PortalControl.portalControlPath
                + "logs", "install_chameleon.log");
        try {
            new CheckTaskMysqlFullMigration().checkChameleonVersion(chameleonVersionOrder, chameleonInstallLogPath);
        } catch (PortalException e) {
            this.setStatus(1);
            this.setMsg(Check.CheckPortalStatus.CHECK_STATUS_PREFIX
                    + Check.CheckPortalStatus.CHECK_STATUS_CHAMELEON_ERR);
        }
    }
}
