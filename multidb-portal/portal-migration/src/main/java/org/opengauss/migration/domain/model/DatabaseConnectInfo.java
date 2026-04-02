/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.domain.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Database server connection information
 *
 * @since 2025/7/1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseConnectInfo {
    /**
     * Database server ip
     */
    private String ip;

    /**
     * Database server port
     */
    private String port;

    /**
     * Database name
     */
    private String databaseName;

    /**
     * Database connect username
     */
    private String username;

    /**
     * Database connect user password
     */
    private String password;
}
