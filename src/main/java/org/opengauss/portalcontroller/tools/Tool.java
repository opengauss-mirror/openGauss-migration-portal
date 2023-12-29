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

package org.opengauss.portalcontroller.tools;

import org.opengauss.portalcontroller.exception.PortalException;

/**
 * Tool interface
 *
 * @date :2023/11/3 15:22
 * @description: Tool interface
 * @version: 1.1
 * @since 1.1
 */
public interface Tool {
    /**
     * Install all packages boolean.
     *
     * @param isDownload the isDownload
     * @return boolean
     */
    boolean install(boolean isDownload);

    /**
     * Prepare work.
     *
     * @param workspaceId the workspace id
     * @return boolean
     */
    boolean init(String workspaceId);

    /**
     * Start.
     *
     * @param workspaceId the workspace id
     * @return boolean
     */
    boolean start(String workspaceId);

    /**
     * Check end.
     *
     * @return boolean
     */
    boolean stop();

    /**
     * Uninstall.
     *
     * @return boolean
     */
    boolean uninstall();

    /**
     * checkStatus
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    boolean checkStatus(String workspaceId);

    /**
     * reportProgress
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    boolean reportProgress(String workspaceId);
}
