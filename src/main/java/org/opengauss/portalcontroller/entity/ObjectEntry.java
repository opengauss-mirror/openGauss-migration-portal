/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.entity;

import lombok.Data;

/**
 * 全量迁移状态实体解析对象 对象详细信息
 */
@Data
public class ObjectEntry {
    /**
     * 对象名称
     */
    private String name;
    /**
     * 状态（1：待迁移，2：迁移中，3：迁移完成，6：迁移失败）
     */
    private int status;
    /**
     * 迁移进度（小于1时处于正常范围，status为6时可以是大于1的值）
     */
    private float percent;
    /**
     * 错误信息，如果对象迁移失败，会输出错误信息，默认为""
     */
    private String error;


}
