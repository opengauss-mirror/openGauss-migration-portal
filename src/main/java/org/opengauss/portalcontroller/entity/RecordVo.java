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

import java.util.List;

/**
 * 全量迁移状态实体解析对象
 */
@Data
public class RecordVo {
    private Total total;
    private List<ObjectEntry> table;
    private List<ObjectEntry> view;
    private List<ObjectEntry> function;
    private List<ObjectEntry> trigger;
    private List<ObjectEntry> procedure;
}
