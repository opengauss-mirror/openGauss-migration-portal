/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
