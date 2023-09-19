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

import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.ConnectorMysql;
import org.opengauss.portalcontroller.software.ConnectorOpengauss;
import org.opengauss.portalcontroller.software.Datacheck;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * NecessaryFileCheckStatusNode
 *
 * @since 1.1
 * @date :2023/9/19 16:22
 * @description: NecessaryFileCheckStatusNode
 * @version: 1.1
 */
public class NecessaryFileCheckStatusNode extends BaseCheckNode {
    @Override
    public void checkStatus() {
        List<String> fileList = new ArrayList<>();
        fileList.addAll(new Confluent().initCriticalFileList());
        fileList.addAll(new ConnectorMysql().initCriticalFileList());
        fileList.addAll(new ConnectorOpengauss().initCriticalFileList());
        fileList.addAll(new Datacheck().initCriticalFileList());
        for (String path : fileList) {
            File file = new File(path);
            if (!file.exists()) {
                this.setStatus(1);
                this.setMsg(Check.CheckPortalStatus.CHECK_STATUS_PREFIX
                        + path + Check.CheckPortalStatus.CHECK_STATUS_FILE_NOT_EXIST_ERR);
                break;
            }
        }
    }
}
