/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
