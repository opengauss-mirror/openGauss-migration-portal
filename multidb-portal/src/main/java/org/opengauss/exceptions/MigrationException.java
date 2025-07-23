/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * migration exception
 *
 * @since 2025/4/30
 */
public class MigrationException extends RuntimeException {
    public MigrationException(String msg) {
        super(msg);
    }

    public MigrationException(Throwable throwable) {
        super(throwable);
    }

    public MigrationException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
