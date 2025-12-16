/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.opengauss.migration.verify.AbstractVerifyChain;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * Abstract mysql verify chain
 *
 * @since 2025/5/8
 */
public abstract class AbstractMysqlVerifyChain extends AbstractVerifyChain {
    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        if (!(verifyDto instanceof MysqlVerifyDto)) {
            throw new IllegalArgumentException("VerifyDto must be MysqlVerifyDto");
        }
        doVerify((MysqlVerifyDto) verifyDto, verifyResult);
    }

    abstract void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult);
}
