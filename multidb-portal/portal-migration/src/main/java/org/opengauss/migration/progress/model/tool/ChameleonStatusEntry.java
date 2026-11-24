/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.progress.model.tool;

import lombok.Data;
import org.opengauss.migration.progress.model.FullEntry;
import org.opengauss.migration.progress.model.FullTotalInfo;

import java.util.List;

/**
 * chameleon status entry
 *
 * @since 2025/6/3
 */
@Data
public class ChameleonStatusEntry {
    private FullTotalInfo total;
    private List<FullEntry> table;
    private List<FullEntry> view;
    private List<FullEntry> function;
    private List<FullEntry> trigger;
    private List<FullEntry> procedure;
}
