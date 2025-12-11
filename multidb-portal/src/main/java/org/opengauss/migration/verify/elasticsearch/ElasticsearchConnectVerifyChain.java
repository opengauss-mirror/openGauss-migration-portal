/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.opengauss.domain.database.connect.info.ElasticsearchConnectInfo;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.ElasticsearchVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.ElasticsearchUtils;

import java.io.IOException;

/**
 * ElasticsearchConnectVerifyChain
 *
 * @since 2025/10/30
 */
public class ElasticsearchConnectVerifyChain extends AbstractElasticsearchVerifyChain {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchConnectVerifyChain.class);
    private static final String VERIFY_NAME = "Elasticsearch Connect Verify";

    @Override
    public void doVerify(ElasticsearchVerifyDto verifyDto, VerifyResult verifyResult) {
        chainResult.setName(VERIFY_NAME);

        ElasticsearchConnectInfo connectInfo = verifyDto.getMigrationConfigDto().getElasticsearchConnectInfo();
        try {
            RestClient restClient = ElasticsearchUtils.createRestClient(connectInfo.getIp(),
                    Integer.parseInt(connectInfo.getPort()), connectInfo.getUsername(), connectInfo.getPassword());
            int statusCode = ElasticsearchUtils.healthStatusCode(restClient);
            if (statusCode >= 200 && statusCode < 300) {
                verifyDto.setRestClient(restClient);
            } else {
                chainResult.setSuccess(false);
                chainResult.setDetail("Get '/_cluster/health' status code is not 2xx, status code: " + statusCode);
                return;
            }
        } catch (IOException e) {
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
