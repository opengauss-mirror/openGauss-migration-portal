/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.opengauss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.PGProperty;
import org.opengauss.domain.database.connect.info.OpenGaussDatabaseConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * openGauss Create Replication Connection Verify Chain
 *
 * @since 2025/6/7
 */
public class OpenGaussReplicationConnectionVerifyChain extends AbstractOpenGaussVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(OpenGaussReplicationConnectionVerifyChain.class);
    private static final String VERIFY_NAME = "OpenGauss Connect User Create Replication Connection Verify";

    @Override
    public void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        doVerify(verifyDto);
        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }

    private void doVerify(AbstractVerifyDto verifyDto) {
        OpenGaussDatabaseConnectInfo connectInfo = verifyDto.getMigrationConfigDto().getOpenGaussConnectInfo();
        String openGaussIp = connectInfo.getIp();
        String openGaussDatabaseName = connectInfo.getDatabaseName();

        Properties properties = new Properties();
        PGProperty.USER.set(properties, connectInfo.getUsername());
        PGProperty.PASSWORD.set(properties, connectInfo.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");

        int port = Integer.parseInt(connectInfo.getPort());
        int haPort = port + 1;
        String url = String.format(Locale.ROOT, "jdbc:opengauss://%s:%d/%s", openGaussIp, port, openGaussDatabaseName);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            chainResult.setSuccess(true);
        } catch (SQLException e) {
            url = String.format(Locale.ROOT, "jdbc:opengauss://%s:%d/%s", openGaussIp, haPort, openGaussDatabaseName);
            try (Connection connection = DriverManager.getConnection(url, properties)) {
                chainResult.setSuccess(true);
            } catch (SQLException ex) {
                String errorMsg = String.format(VerifyConstants.SQL_EXCEPTION_MODEL, ex.getMessage());
                LOGGER.error(errorMsg, ex);
                chainResult.setSuccess(false);
                chainResult.setDetail(errorMsg);
            }
        }
    }
}
