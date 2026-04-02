/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
