/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.status.model;

import lombok.Data;
import org.opengauss.migration.progress.model.CheckFailEntry;
import org.opengauss.migration.progress.model.FullEntry;

/**
 * Object status entry
 *
 * @since 2025/7/15
 */
@Data
public class ObjectStatusEntry {
    private String schema;
    private String name;
    private String type;

    /**
     * status: 1 - pending, 2 - migrating, 3,4,5 - completed, 6,7 - failed
     */
    private int status;
    private double percent;
    private String error;

    /**
     * check status: 0 - success, 1 - fail
     */
    private Integer checkStatus;
    private String checkMessage;
    private String repairFilePath;

    public ObjectStatusEntry(FullEntry fullEntry, String type) {
        this.type = type;
        this.schema = fullEntry.getSchema();
        this.name = fullEntry.getName();
        this.status = fullEntry.getStatus();
        this.percent = fullEntry.getPercent();
        this.error = fullEntry.getError();
    }

    /**
     * Set check success status
     */
    public void setCheckSuccessStatus() {
        this.checkStatus = 0;
    }

    /**
     * Set check fail status
     *
     * @param checkFailEntry check fail entry
     */
    public void setCheckFailStatus(CheckFailEntry checkFailEntry) {
        this.checkStatus = 1;
        this.checkMessage = checkFailEntry.getError();
        this.repairFilePath = checkFailEntry.getRepairFilePath();
    }

    /**
     * Compare object status entry
     *
     * @param o1 object status entry 1
     * @param o2 object status entry 2
     * @return int compare result
     */
    public static int compare(ObjectStatusEntry o1, ObjectStatusEntry o2) {
        if (o1.getSchema().equals(o2.getSchema())) {
            return o1.getName().compareTo(o2.getName());
        } else {
            return o1.getSchema().compareTo(o2.getSchema());
        }
    }

    /**
     * Compare object status entry by name
     *
     * @param o1 object status entry 1
     * @param o2 object status entry 2
     * @return int compare result
     */
    public static int compareByName(ObjectStatusEntry o1, ObjectStatusEntry o2) {
        return o1.getName().compareTo(o2.getName());
    }
}
