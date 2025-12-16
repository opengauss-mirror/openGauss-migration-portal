/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.ChainResult;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MysqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mysql full migration permission verify chain
 *
 * @since 2025/6/7
 */
public class MysqlFullPermissionVerifyChain extends AbstractMysqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MysqlFullPermissionVerifyChain.class);
    private static final String VERIFY_NAME = "MySQL Connect User Full Migration Permission Verify";
    private static final String[] PERMISSION_COLUMN = {
            VerifyConstants.MYSQL_PERMISSION_SELECT, VerifyConstants.MYSQL_PERMISSION_RELOAD,
            VerifyConstants.MYSQL_PERMISSION_REP_CLIENT, VerifyConstants.MYSQL_PERMISSION_LOCK_TABLES
    };

    @Override
    public void doVerify(MysqlVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        verifyPermission(PERMISSION_COLUMN, verifyDto, chainResult);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }

    /**
     * Verify connect user permission
     *
     * @param permissions permission column array
     * @param verifyDto verify dto
     * @param verifyChainResult verify chain result
     */
    protected void verifyPermission(String[] permissions, MysqlVerifyDto verifyDto, ChainResult verifyChainResult) {
        try {
            Connection connection = verifyDto.getMysqlConnection();
            String sourceUsername = verifyDto.getMigrationConfigDto().getMysqlDatabaseUsername();
            StringBuilder detailBuilder = new StringBuilder("Does not have the following permissions: ");
            for (String permission : permissions) {
                if (!MysqlUtils.hasPermission(permission, sourceUsername, connection)) {
                    verifyChainResult.setSuccess(false);
                    detailBuilder.append(permission).append(", ");
                }
            }

            if (!verifyChainResult.isSuccess()) {
                verifyChainResult.setDetail(detailBuilder.substring(0, detailBuilder.length() - 2));
            }
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            verifyChainResult.setSuccess(false);
            verifyChainResult.setDetail(errorMsg);
        }
    }
}
