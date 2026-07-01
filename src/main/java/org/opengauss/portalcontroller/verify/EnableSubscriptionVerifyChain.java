/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Verify whether the enable_subscription is on
 *
 * @since 2026/3/5
 */
public class EnableSubscriptionVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnableSubscriptionVerifyChain.class);
    private static final String ENABLE_SUBSCRIPTION = "enable_subscription";

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        doVerify(resultMap, pgConnection);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private void doVerify(Map<String, Object> resultMap, PgConnection pgConnection) {
        Map<String, Object> paramMap = new HashMap<>();
        if (pgConnection == null) {
            resultMap.put(ENABLE_SUBSCRIPTION, paramMap);
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
            setVerifyResult(resultMap, false);
            return;
        }

        try {
            if (!isEnableSubscriptionSupported(pgConnection)) {
                return;
            }
        } catch (SQLException e) {
            resultMap.put(ENABLE_SUBSCRIPTION, paramMap);
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message", String.format("Failed to get openGauss version, error: %s", e.getMessage()));
            LOGGER.error("Failed to get openGauss version, error: {}", e.getMessage(), e);
            setVerifyResult(resultMap, false);
            return;
        }

        resultMap.put(ENABLE_SUBSCRIPTION, paramMap);
        boolean isOn = false;
        try {
            String enableSubscription = getEnableSubscriptionValue(pgConnection);
            isOn = enableSubscription.equals("on");
            if (isOn) {
                paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            } else {
                paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
                paramMap.put("expected_value", "on");
            }
        } catch (SQLException e) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message",
                    String.format("Failed to get enable_subscription value, error: %s", e.getMessage()));
            LOGGER.error("Failed to get enable_subscription value", e);
        }
        setVerifyResult(resultMap, isOn);
    }

    private String getEnableSubscriptionValue(PgConnection pgConnection) throws SQLException {
        String selectSql = String.format(Constants.SHOW_OPENGAUSS_GUC_PARAM, ENABLE_SUBSCRIPTION);
        return JdbcUtils.selectStringValue(pgConnection, selectSql, ENABLE_SUBSCRIPTION);
    }

    private boolean isEnableSubscriptionSupported(PgConnection pgConnection) throws SQLException {
        String versionNumber = JdbcUtils.getOpenGaussVersionNumber(pgConnection);
        return versionNumber.compareTo("6.0.5") >= 0 && !versionNumber.equals("7.0.0-RC1")
                && !versionNumber.equals("7.0.0-RC2");
    }
}
