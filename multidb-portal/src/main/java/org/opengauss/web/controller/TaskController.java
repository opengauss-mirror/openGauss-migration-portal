/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.controller;

import io.smallrye.faulttolerance.api.RateLimit;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.opengauss.web.service.TaskService;
import org.opengauss.web.thread.StopQuarkusThread;

import java.time.temporal.ChronoUnit;

/**
 * Task controller
 *
 * @since 2025/5/21
 */
@Path("/task")
@Produces(MediaType.TEXT_PLAIN)
public class TaskController {
    private static final String SUCCESS = "SUCCESS";

    @Inject
    @Named("taskService")
    TaskService taskService;

    @Inject
    @Named("stopQuarkusThread")
    StopQuarkusThread stopQuarkusThread;

    /**
     * Stop incremental migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/stopIncremental")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String stopIncremental() {
        taskService.stopIncremental();
        return SUCCESS;
    }

    /**
     * Resume incremental migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/resumeIncremental")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String resumeIncremental() {
        taskService.resumeIncremental();
        return SUCCESS;
    }

    /**
     * Restart incremental migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/restartIncremental")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String restartIncremental() {
        taskService.restartIncremental();
        return SUCCESS;
    }

    /**
     * Start reverse migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/startReverse")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String startReverse() {
        taskService.startReverse();
        return SUCCESS;
    }

    /**
     * Stop reverse migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/stopReverse")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String stopReverse() {
        taskService.stopReverse();
        return SUCCESS;
    }

    /**
     * Resume reverse migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/resumeReverse")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String resumeReverse() {
        taskService.resumeReverse();
        return SUCCESS;
    }

    /**
     * Restart reverse migration
     *
     * @return SUCCESS
     */
    @POST
    @Path("/restartReverse")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String restartReverse() {
        taskService.restartReverse();
        return SUCCESS;
    }

    /**
     * Stop task
     *
     * @return SUCCESS
     */
    @POST
    @Path("/stop")
    @RateLimit(
            value = 1,
            window = 5,
            windowUnit = ChronoUnit.SECONDS
    )
    public String stopTask() {
        stopQuarkusThread.start();
        return SUCCESS;
    }
}
