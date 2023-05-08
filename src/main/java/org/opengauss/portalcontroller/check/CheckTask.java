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

package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.constant.Command.Install.Mysql.FullMigration;
/**
 * The interface Check task.
 */
public interface CheckTask {

    /**
     * Install all packages boolean.
     *
     * @param download the download
     * @return the boolean
     */
    void installAllPackages(boolean download) throws PortalException;

    /**
     * Change parameters.
     *
     * @param workspaceId the workspace id
     * @throws PortalException the portal exception
     */
    void changeParameters(String workspaceId) throws PortalException;

    /**
     * Prepare work.
     *
     * @param workspaceId the workspace id
     */
    void prepareWork(String workspaceId);

    /**
     * Start.
     *
     * @param workspaceId the workspace id
     */
    void start(String workspaceId);

    /**
     * Check end.
     */
    void checkEnd();

    /**
     * Uninstall.
     */
    void uninstall();
}
