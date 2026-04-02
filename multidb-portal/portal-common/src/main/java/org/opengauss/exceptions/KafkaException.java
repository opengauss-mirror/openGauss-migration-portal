/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.exceptions;

/**
 * kafka exception
 *
 * @since 2025/4/18
 */
public class KafkaException extends RuntimeException {
    public KafkaException(String msg) {
        super(msg);
    }

    public KafkaException(Throwable throwable) {
        super(throwable);
    }

    public KafkaException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
