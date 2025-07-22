/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * Portal exception
 *
 * @since 2025/6/5
 */
public class PortalException extends RuntimeException {
    public PortalException(String message) {
        super(message);
    }

    public PortalException(Throwable throwable) {
        super(throwable);
    }

    public PortalException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
