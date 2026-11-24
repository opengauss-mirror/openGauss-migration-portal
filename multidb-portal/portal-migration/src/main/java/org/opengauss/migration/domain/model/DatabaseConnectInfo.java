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
