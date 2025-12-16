/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.OpenGaussUtils;
import org.opengauss.utils.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * openGauss Full Permission Verify Chain
 *
 * @since 2025/6/7
 */
public class OpenGaussFullPermissionVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussFullPermissionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Connect User Full Migration Permission Verify";
    private static final String[] PERMISSION_LIST = {
            VerifyConstants.OPENGAUSS_PERMISSION_CREATE
    };

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        verifyPermission(PERMISSION_LIST, verifyDto);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }

    /**
     * Verify openGauss connect user permission
     *
     * @param permissionList permission list
     * @param verifyDto verify dto
     */
    protected void verifyPermission(String[] permissionList, AbstractVerifyDto verifyDto) {
        if (isSystemAdmin(verifyDto)) {
            return;
        }

        try {
            OpenGaussDatabaseConnectInfo connectInfo = verifyDto.getMigrationConfigDto().getOpenGaussConnectInfo();
            String permissions = OpenGaussUtils.getDatabaseAccessPermissions(connectInfo.getDatabaseName(),
                    verifyDto.getOpengaussConnection());
            StringBuilder detailBuilder = new StringBuilder("Does not have the following permissions: ");
            if (StringUtils.isNullOrBlank(permissions)) {
                chainResult.setSuccess(false);
                for (String permission : permissionList) {
                    detailBuilder.append(permission).append(", ");
                }
            } else {
                String userPermission = parseUserPermission(permissions, connectInfo.getUsername());
                for (String permission : permissionList) {
                    if (!userPermission.contains(permission)) {
                        chainResult.setSuccess(false);
                        detailBuilder.append(permission).append(", ");
                    }
                }
            }

            if (!chainResult.isSuccess()) {
                chainResult.setDetail(detailBuilder.append(VerifyConstants.OPENGAUSS_PERMISSION_DESC).toString());
            }
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }
    }

    /**
     * Check if the user is a system administrator
     *
     * @param verifyDto verify dto
     * @return true if the user is a system administrator, false otherwise
     */
    protected boolean isSystemAdmin(AbstractVerifyDto verifyDto) {
        Connection connection = verifyDto.getOpengaussConnection();
        String username = verifyDto.getMigrationConfigDto().getOpenGaussConnectInfo().getUsername();
        try {
            return OpenGaussUtils.isSystemAdmin(username, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to check {} sysadmin permission", username, e);
        }
        return false;
    }

    private static String parseUserPermission(String permissions, String username) {
        StringBuilder userPermissionStr = new StringBuilder();
        String[] userPermissions = permissions.split(",");

        for (String userPermission : userPermissions) {
            String[] permissionParts = userPermission.split("=");
            if (permissionParts.length == 2) {
                String user = permissionParts[0];
                String permission = permissionParts[1].substring(0, permissionParts[1].indexOf("/"));
                if (user.equals(username)) {
                    userPermissionStr.append(permission);
                }
            }
        }
        return userPermissionStr.toString();
    }
}
