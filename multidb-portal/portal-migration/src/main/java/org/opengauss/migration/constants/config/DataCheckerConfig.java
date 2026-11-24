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

package org.opengauss.migration.constants.config;

/**
 * DataCheckerConfig
 *
 * @since 2025/9/16
 */
public class DataCheckerConfig {
    /**
     * enable env password
     */
    public static final String ENABLE_ENV_PASSWORD = "enable.env.password";

    /**
     * env database password
     */
    public static final String ENV_DATABASE_PASSWORD = "spring.datasource.password";

    private DataCheckerConfig() {
    }
}
