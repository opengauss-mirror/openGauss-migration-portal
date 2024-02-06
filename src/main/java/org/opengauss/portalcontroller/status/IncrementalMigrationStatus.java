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

package org.opengauss.portalcontroller.status;

/**
 * The type Incremental migration status.
 */
public class IncrementalMigrationStatus {
    private int status;
    private int count;
    private int sourceSpeed;
    private int sinkSpeed;
    private int rest;
    private int failCount;
    private int successCount;
    private int replayedCount;
    private int skippedCount;
    private String msg;

    public IncrementalMigrationStatus() {
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public int getStatus() {
        return status;
    }

    /**
     * Sets status.
     *
     * @param status the status
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Gets count.
     *
     * @return the count
     */
    public int getCount() {
        return count;
    }

    /**
     * Sets count.
     *
     * @param count the count
     */
    public void setCount(int count) {
        this.count = count;
    }

    /**
     * Gets source speed.
     *
     * @return the source speed
     */
    public int getSourceSpeed() {
        return sourceSpeed;
    }

    /**
     * Sets source speed.
     *
     * @param sourceSpeed the source speed
     */
    public void setSourceSpeed(int sourceSpeed) {
        this.sourceSpeed = sourceSpeed;
    }

    /**
     * Gets sink speed.
     *
     * @return the sink speed
     */
    public int getSinkSpeed() {
        return sinkSpeed;
    }

    /**
     * Sets sink speed.
     *
     * @param sinkSpeed the sink speed
     */
    public void setSinkSpeed(int sinkSpeed) {
        this.sinkSpeed = sinkSpeed;
    }

    /**
     * Gets rest.
     *
     * @return the rest
     */
    public int getRest() {
        return rest;
    }

    /**
     * Sets rest.
     *
     * @param rest the rest
     */
    public void setRest(int rest) {
        this.rest = rest;
    }

    /**
     * Gets msg.
     *
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }

    /**
     * Sets msg.
     *
     * @param msg the msg
     */
    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * Gets fail count.
     *
     * @return the fail count
     */
    public int getFailCount() {
        return failCount;
    }

    /**
     * Sets fail count.
     *
     * @param failCount the fail count
     */
    public void setFailCount(int failCount) {
        this.failCount = failCount;
    }

    /**
     * Gets success count.
     *
     * @return the success count
     */
    public int getSuccessCount() {
        return successCount;
    }

    /**
     * Sets success count.
     *
     * @param successCount the success count
     */
    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    /**
     * Gets replayed count.
     *
     * @return the replayed count
     */
    public int getReplayedCount() {
        return replayedCount;
    }

    /**
     * Sets replayed count.
     *
     * @param replayedCount the replayed count
     */
    public void setReplayedCount(int replayedCount) {
        this.replayedCount = replayedCount;
    }

    /**
     * Gets skipped count.
     *
     * @return the skipped count
     */
    public int getSkippedCount() {
        return skippedCount;
    }

    /**
     * Sets skipped count.
     *
     * @param skippedCount the skipped count
     */
    public void setSkippedCount(int skippedCount) {
        this.skippedCount = skippedCount;
    }
}
