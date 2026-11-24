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

package org.opengauss.exceptions;

/**
 * migration mode exception
 *
 * @since 2025/4/23
 */
public class MigrationModeException extends RuntimeException {
    public MigrationModeException(String message) {
        super(message);
    }

    public MigrationModeException(Throwable e) {
        super(e);
    }

    public MigrationModeException(String message, Throwable e) {
        super(message, e);
    }
}
