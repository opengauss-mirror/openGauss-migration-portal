/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * Mysql reverse permission verify chain
 *
 * @since 2025/6/7
 */
public class MysqlReversePermissionVerifyChain extends MysqlFullPermissionVerifyChain {
    private static final String VERIFY_NAME = "MySQL Connect User Reverse Migration Permission Verify";
    private static final String[] PERMISSION_COLUMN = {
            VerifyConstants.MYSQL_PERMISSION_SELECT, VerifyConstants.MYSQL_PERMISSION_INSERT,
            VerifyConstants.MYSQL_PERMISSION_UPDATE, VerifyConstants.MYSQL_PERMISSION_DELETE
    };

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        verifyPermission(PERMISSION_COLUMN, verifyDto, chainResult);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
