/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.pgsql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.PGProperty;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * PostgreSQL create replication connection verify chain
 *
 * @since 2025/7/9
 */
public class PgsqlReplicationConnectionVerifyChain extends AbstractPgsqlVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(PgsqlReplicationConnectionVerifyChain.class);
    private static final String VERIFY_NAME = "PostgreSQL Connect User Create Replication Connection Verify";

    @Override
    public void verify(VerifyDto verifyDto, VerifyResult verifyResult) {
        verifyDto.checkConnection();
        chainResult.setName(VERIFY_NAME);

        doVerify(verifyDto);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(VerifyDto verifyDto) {
        Properties properties = new Properties();
        PGProperty.USER.set(properties, verifyDto.getSourceUsername());
        PGProperty.PASSWORD.set(properties, verifyDto.getSourcePassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");

        String url = String.format("jdbc:postgresql://%s:%s/%s", verifyDto.getSourceIp(), verifyDto.getSourcePort(),
                verifyDto.getSourceDatabase());
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            chainResult.setSuccess(true);
        } catch (SQLException e) {
            String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }
    }
}
