/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.progress.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

/**
 * full entry
 *
 * @since 2025/6/3
 */
@Data
public class FullEntry {
    /**
     * schema name
     */
    @JSONField(defaultValue = "")
    private String schema;

    /**
     * object name
     */
    private String name;

    /**
     * status: 1 - pending, 2 - migrating, 3,4,5 - completed, 6,7 - failed
     */
    private int status;

    /**
     * migrated percentage, less than 1 when in normal range, status is 6 may be greater than 1
     */
    private double percent;

    /**
     * error message, if object migration failed, will output error message, default is ""
     */
    private String error;

    /**
     * compare full entry
     *
     * @param o1 full entry 1
     * @param o2 full entry 2
     * @return int compare result
     */
    public static int compare(FullEntry o1, FullEntry o2) {
        if (o1.getSchema().equals(o2.getSchema())) {
            return o1.getName().compareTo(o2.getName());
        } else {
            return o1.getSchema().compareTo(o2.getSchema());
        }
    }

    /**
     * compare full entry by name
     *
     * @param o1 full entry 1
     * @param o2 full entry 2
     * @return int compare result
     */
    public static int compareByName(FullEntry o1, FullEntry o2) {
        return o1.getName().compareTo(o2.getName());
    }
}
