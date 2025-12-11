/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.ElasticsearchVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.ElasticsearchUtils;

import java.io.IOException;

/**
 * ElasticsearchVersionVerifyChain
 *
 * @since 2025/10/31
 */
public class ElasticsearchVersionVerifyChain extends AbstractElasticsearchVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchVersionVerifyChain.class);
    private static final String VERIFY_NAME = "Elasticsearch Version Verify";
    private static final String MINIMUM_VERSION = "7.3.0";

    @Override
    void doVerify(ElasticsearchVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        try {
            String version = ElasticsearchUtils.getElasticsearchVersion(verifyDto.getRestClient());
            LOGGER.info("Elasticsearch version: {}", version);

            if (compareVersions(version, MINIMUM_VERSION) < 0) {
                chainResult.setSuccess(false);
                chainResult.setDetail(
                        "Elasticsearch version " + version + " is lower than minimum version " + MINIMUM_VERSION + ".");
            }
        } catch (IOException e) {
            String errorMsg = String.format(VerifyConstants.EXCEPTION_MODEL, e.getMessage());
            LOGGER.error(errorMsg, e);
            chainResult.setSuccess(false);
            chainResult.setDetail(errorMsg);
        }

        addCurrentChainResult(verifyResult);
        transfer(verifyDto, verifyResult);
    }
}
