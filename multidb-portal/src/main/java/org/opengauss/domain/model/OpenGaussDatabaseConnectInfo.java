/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

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
