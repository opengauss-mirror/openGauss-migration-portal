/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * config exception
 *
 * @since 2025/2/27
 */
public class ConfigException extends RuntimeException {
    public ConfigException(String msg) {
        super(msg);
    }

    public ConfigException(Throwable throwable) {
        super(throwable);
    }

    public ConfigException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
