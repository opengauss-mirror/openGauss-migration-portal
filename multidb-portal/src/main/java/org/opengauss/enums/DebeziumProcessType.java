/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.Getter;

/**
 * debezium process type
 *
 * @since 2025/5/19
 */
@Getter
public enum DebeziumProcessType {
    SINK("sink"),
    SOURCE("source"),
    ;

    DebeziumProcessType(String type) {
        this.type = type;
    }

    private final String type;
}
