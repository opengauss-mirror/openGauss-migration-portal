/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
