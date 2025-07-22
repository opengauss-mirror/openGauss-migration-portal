/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

/**
 * control stop order
 *
 * @since 2025/3/1
 */
public class MigrationStopIndicator {
    private volatile boolean isStop;

    public MigrationStopIndicator() {
        isStop = false;
    }

    /**
     * is stopped
     *
     * @return boolean is stopped
     */
    public boolean isStopped() {
        return isStop;
    }

    /**
     * set stop
     */
    public void setStop() {
        isStop = true;
    }
}
