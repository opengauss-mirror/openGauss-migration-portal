/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * AbstractPreMigrationVerifyChain
 *
 * @date :2023/11/3 15:22
 * @description: AbstractPreMigrationVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public abstract class AbstractPreMigrationVerifyChain {
    /**
     * transfer constants
     */
    protected AbstractPreMigrationVerifyChain next;

    /**
     * verify migration must be ok
     *
     * @param resultMap       result Map
     * @param mysqlConnection mysql connect
     * @param pgConnection    openGauss connect
     */
    public abstract void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection);

    /**
     * transfer parameter and chain
     *
     * @param resultMap       result Map
     * @param mysqlConnection mysql connect
     * @param pgConnection    openGauss connect
     */
    protected void transfer(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        if (next != null) {
            next.verify(resultMap, mysqlConnection, pgConnection);
        }
    }

    /**
     * Check openGauss version later than 7.0.0-RC3
     *
     * @param pgConnection openGauss connect
     * @return true if openGauss version later than 7.0.0-RC3
     * @throws SQLException SQL Exception
     */
    protected boolean isOpenGaussLaterThan700RC3(PgConnection pgConnection) throws SQLException {
        if (pgConnection == null) {
            throw new SQLException("Connection is null");
        }
        String versionNumber = JdbcUtils.getOpenGaussVersionNumber(pgConnection);
        return versionNumber.compareTo("7.0.0") >= 0 && !versionNumber.equals("7.0.0-RC1")
                && !versionNumber.equals("7.0.0-RC2");
    }

    /**
     * Set verify result to resultMap
     *
     * @param resultMap result map
     * @param isPassedVerify true if verify passed
     */
    protected void setVerifyResult(Map<String, Object> resultMap, boolean isPassedVerify) {
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isPassedVerify ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE));
    }
}
