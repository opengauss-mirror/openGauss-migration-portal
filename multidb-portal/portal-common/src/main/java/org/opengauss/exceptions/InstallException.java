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
 * install exception
 *
 * @since 2025/4/15
 */
public class InstallException extends RuntimeException {
    public InstallException(String msg) {
        super(msg);
    }

    public InstallException(Throwable throwable) {
        super(throwable);
    }

    public InstallException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
