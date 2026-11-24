/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.domain.model;

import lombok.Data;

/**
 * openGauss database connect information
 *
 * @since 2025/7/1
 */
@Data
public class OpenGaussDatabaseConnectInfo extends DatabaseConnectInfo {
    private boolean isClusterAvailable;
    private String standbyHosts;
    private String standbyPorts;

    public OpenGaussDatabaseConnectInfo(String ip, String port, String databaseName, String username, String password,
        boolean isClusterAvailable, String standbyHosts, String standbyPorts) {
        super(ip, port, databaseName, username, password);
        this.isClusterAvailable = isClusterAvailable;
        this.standbyHosts = standbyHosts;
        this.standbyPorts = standbyPorts;
    }
}
