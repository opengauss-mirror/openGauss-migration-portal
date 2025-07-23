/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify;

import org.opengauss.migration.verify.model.ChainResult;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * Abstract verify chain
 *
 * @since 2025/5/8
 */
public abstract class AbstractVerifyChain {
    /**
     * Chain result
     */
    protected final ChainResult chainResult = new ChainResult();

    /**
     * Next verify chain
     */
    protected AbstractVerifyChain next;

    /**
     * Verify
     *
     * @param verifyDto verify dto
     * @param verifyResult verify result
     */
    public abstract void verify(VerifyDto verifyDto, VerifyResult verifyResult);

    /**
     * Transfer to next verify chain
     *
     * @param verifyDto verify dto
     * @param verifyResult verify result
     */
    protected final void transfer(VerifyDto verifyDto, VerifyResult verifyResult) {
        if (this.next != null) {
            this.next.verify(verifyDto, verifyResult);
        }
    }

    /**
     * Add current chain result to verify result
     *
     * @param verifyResult verify result
     */
    protected final void addCurrentChainResult(VerifyResult verifyResult) {
        verifyResult.addChainResult(this.chainResult);
    }
}
