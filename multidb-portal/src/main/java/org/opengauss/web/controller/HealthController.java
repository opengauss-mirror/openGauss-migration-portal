/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.controller;

import io.smallrye.faulttolerance.api.RateLimit;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.temporal.ChronoUnit;

/**
 * Health controller
 *
 * @since 2025/3/3
 */
@Path("/health")
public class HealthController {
    /**
     * Check health
     *
     * @return Portal is UP
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @RateLimit(
            value = 10,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String health() {
        return "Portal is UP";
    }
}
