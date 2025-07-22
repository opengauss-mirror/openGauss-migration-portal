/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * task exception
 *
 * @since 2025/4/24
 */
public class TaskException extends RuntimeException {
    public TaskException(String msg) {
        super(msg);
    }

    public TaskException(Throwable throwable) {
        super(throwable);
    }

    public TaskException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
