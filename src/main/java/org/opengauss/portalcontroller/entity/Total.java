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
 * 全量迁移状态实体解析对象 对象汇总信息
 */
@Data
public class Total {
    /**
     * 所有表的总记录数，预估值
     */
    private int record;
    /**
     * 所有表的数据总量，预估值
     */
    private String data;
    /**
     * 迁移总耗时
     */
    private int time;
    /**
     * 迁移速率
     */
    private String speed;
}
