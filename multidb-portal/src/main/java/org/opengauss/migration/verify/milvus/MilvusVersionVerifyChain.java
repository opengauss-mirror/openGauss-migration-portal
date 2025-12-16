/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.milvus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.MilvusVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.MilvusUtils;

/**
 * MilvusVersionVerifyChain
 *
 * @since 2025/11/1
 */
public class MilvusVersionVerifyChain extends AbstractMilvusVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(MilvusVersionVerifyChain.class);
    private static final String VERIFY_NAME = "Milvus Version Verify";
    private static final String MINIMUM_VERSION = "2.3.0";

    @Override
    void doVerify(MilvusVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            String milvusVersion = MilvusUtils.getMilvusVersion(verifyDto.getMilvusClientV2());
            LOGGER.info("Milvus version: {}", milvusVersion);

            if (compareVersions(milvusVersion, MINIMUM_VERSION) < 0) {
                chainResult.setSuccess(false);
                chainResult.setDetail(
                        "Milvus version " + milvusVersion + " is lower than minimum version " + MINIMUM_VERSION + ".");
            }
        } catch (Exception e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }

        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
