/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.enums;

import lombok.Getter;

/**
 * install way: offline, online
 *
 * @since 2024/12/24
 */
@Getter
public enum InstallWay {
    OFFLINE("offline"), ONLINE("online");
    private final String name;

    InstallWay(String name) {
        this.name = name;
    }
}
