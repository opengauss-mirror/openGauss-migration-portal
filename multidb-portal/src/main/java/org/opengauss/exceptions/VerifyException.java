/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * verify exception
 *
 * @since 2025/6/7
 */
public class VerifyException extends RuntimeException {
    public VerifyException(String msg) {
        super(msg);
    }

    public VerifyException(Throwable throwable) {
        super(throwable);
    }

    public VerifyException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
