/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

/**
 * Mysql incremental permission verify chain
 *
 * @since 2025/6/7
 */
public class MysqlIncrementalPermissionVerifyChain extends MysqlFullPermissionVerifyChain {
    private static final String VERIFY_NAME = "MySQL Connect User Incremental Migration Permission Verify";
    private static final String[] PERMISSION_COLUMN = {
            VerifyConstants.MYSQL_PERMISSION_SELECT, VerifyConstants.MYSQL_PERMISSION_REP_SLAVE,
            VerifyConstants.MYSQL_PERMISSION_REP_CLIENT
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
