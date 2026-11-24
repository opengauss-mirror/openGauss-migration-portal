/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
