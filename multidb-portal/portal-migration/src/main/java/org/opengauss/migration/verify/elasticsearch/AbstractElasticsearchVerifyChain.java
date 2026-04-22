/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.elasticsearch;

import org.opengauss.migration.verify.AbstractVerifyChain;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.ElasticsearchVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * AbstractElasticsearchVerifyChain
 *
 * @since 2025/10/30
 */
public abstract class AbstractElasticsearchVerifyChain extends AbstractVerifyChain {
    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        if (!(verifyDto instanceof ElasticsearchVerifyDto elasticsearchVerifyDto)) {
            throw new IllegalArgumentException("VerifyDto must be ElasticsearchVerifyDto");
        }
        doVerify(elasticsearchVerifyDto, verifyResult);
    }

    abstract void doVerify(ElasticsearchVerifyDto verifyDto, VerifyResult verifyResult);
}
