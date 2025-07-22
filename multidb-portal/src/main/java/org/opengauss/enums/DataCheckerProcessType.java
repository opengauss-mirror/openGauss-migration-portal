/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.Getter;

/**
 * data checker process type
 *
 * @since 2025/5/14
 */
@Getter
public enum DataCheckerProcessType {
    SINK("sink"),
    SOURCE("source"),
    CHECK("check")
    ;

    DataCheckerProcessType(String type) {
        this.type = type;
    }

    private final String type;
}
