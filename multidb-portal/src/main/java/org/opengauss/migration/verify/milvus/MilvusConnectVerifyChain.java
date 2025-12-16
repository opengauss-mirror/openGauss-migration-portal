/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.milvus;

import io.milvus.v2.client.MilvusClientV2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.database.connect.info.DatabaseConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MilvusVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MilvusUtils;

/**
 * MilvusConnectVerifyChain
 *
 * @since 2025/11/1
 */
public class MilvusConnectVerifyChain extends AbstractMilvusVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MilvusConnectVerifyChain.class);
    private static final String VERIFY_NAME = "Milvus Connect Verify";

    @Override
    void doVerify(MilvusVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        DatabaseConnectInfo connectInfo = verifyDto.getMigrationConfigDto().getMilvusConnectInfo();
        try {
            MilvusClientV2 milvusClientV2 = MilvusUtils.createMilvusClientV2(
                    connectInfo.getIp(), Integer.parseInt(connectInfo.getPort()), connectInfo.getDatabaseName(),
                    connectInfo.getUsername(), connectInfo.getPassword()
            );
            verifyDto.setMilvusClientV2(milvusClientV2);
        } catch (Exception e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
            return;
        } finally {
            addCurrentChainResult(verifyResult);
        }

        transfer(verifyDto, verifyResult);
    }
}
