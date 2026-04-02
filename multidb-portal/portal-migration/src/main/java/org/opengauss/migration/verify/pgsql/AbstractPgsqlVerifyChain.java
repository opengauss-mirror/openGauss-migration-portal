/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.pgsql;

import org.opengauss.migration.verify.AbstractVerifyChain;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.PgsqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * Abstract pgsql verify chain
 *
 * @since 2025/5/8
 */
public abstract class AbstractPgsqlVerifyChain extends AbstractVerifyChain {
    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        if (!(verifyDto instanceof PgsqlVerifyDto pgsqlVerifyDto)) {
            throw new IllegalArgumentException("VerifyDto must be PgsqlVerifyDto");
        }

        doVerify(pgsqlVerifyDto, verifyResult);
    }

    abstract void doVerify(PgsqlVerifyDto verifyDto, VerifyResult verifyResult);
}
