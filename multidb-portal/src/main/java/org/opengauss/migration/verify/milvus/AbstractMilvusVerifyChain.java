/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.milvus;

import org.opengauss.migration.verify.AbstractVerifyChain;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.MilvusVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * AbstractMilvusVerifyChain
 *
 * @since 2025/11/1
 */
public abstract class AbstractMilvusVerifyChain extends AbstractVerifyChain {
    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        if (!(verifyDto instanceof MilvusVerifyDto)) {
            throw new IllegalArgumentException("VerifyDto must be MilvusVerifyDto");
        }
        doVerify((MilvusVerifyDto) verifyDto, verifyResult);
    }

    abstract void doVerify(MilvusVerifyDto verifyDto, VerifyResult verifyResult);
}
