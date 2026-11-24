/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.quarkus.handler;

import io.smallrye.faulttolerance.api.RateLimitException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Rate limit exception handler
 *
 * @since 2025/7/14
 */
@Provider
public class RateLimitExceptionHandler implements ExceptionMapper<RateLimitException> {
    private static final Logger LOGGER = LogManager.getLogger(RateLimitExceptionHandler.class);

    @Override
    public Response toResponse(RateLimitException e) {
        LOGGER.warn("Request too frequent, reason: {}", e.getMessage());
        return Response.status(Response.Status.TOO_MANY_REQUESTS)
                .entity("Request too frequent, please try again after 5 minutes")
                .build();
    }
}
