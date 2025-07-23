/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.handler;

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
