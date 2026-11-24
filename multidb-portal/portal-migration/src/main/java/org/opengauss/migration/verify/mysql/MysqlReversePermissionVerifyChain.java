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

package org.opengauss.migration.verify.mysql;

import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
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
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        verifyPermission(PERMISSION_COLUMN, verifyDto, chainResult);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
