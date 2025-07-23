/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

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
    protected String ip;

    /**
     * Database server port
     */
    protected String port;

    /**
     * Database name
     */
    protected String databaseName;

    /**
     * Database connect username
     */
    protected String username;

    /**
     * Database connect user password
     */
    protected String password;
}
