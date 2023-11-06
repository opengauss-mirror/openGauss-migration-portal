/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.constant;

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
