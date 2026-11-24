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

package org.opengauss.migration.verify.opengauss;

import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * openGauss incremental permission verify chain
 *
 * @since 2025/6/7
 */
public class OpenGaussIncrementalPermissionVerifyChain extends OpenGaussFullPermissionVerifyChain {
    private static final String VERIFY_NAME = "OpenGauss Connect User Incremental Migration Permission Verify";
    private static final String[] PERMISSION_LIST = {
            VerifyConstants.OPENGAUSS_PERMISSION_CREATE, VerifyConstants.OPENGAUSS_PERMISSION_TEMPORARY,
            VerifyConstants.OPENGAUSS_PERMISSION_CONNECT, VerifyConstants.OPENGAUSS_PERMISSION_ALTER,
            VerifyConstants.OPENGAUSS_PERMISSION_DROP, VerifyConstants.OPENGAUSS_PERMISSION_COMMENT
    };

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        verifyPermission(PERMISSION_LIST, verifyDto);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
