/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.enums;

/**
 * TaskParamType
 *
 * @author: www
 * @date: 2023/11/28 12:04
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
public enum TaskParamType {
    TYPE_STRING(1, "string"),
    TYPE_NUMBER(2, "number"),
    TYPE_BOOLEAN(3, "boolean"),
    TYPE_LIST(4, "List"),
    TYPE_OBJECT_ARR(9, "object_arr");
    private final Integer code;
    private final String command;

    TaskParamType(Integer code, String command) {
        this.code = code;
        this.command = command;
    }

    public Integer getCode() {
        return code;
    }

    public String getCommand() {
        return command;
    }
}
