/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.handler;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.opengauss.web.thread.StartMigrationThread;
import org.opengauss.web.service.TaskService;

/**
 * shutdown handler
 *
 * @since 2025/3/26
 */
@ApplicationScoped
public class ShutdownHandler {
    @Inject
    StartMigrationThread mainServiceThread;

    @Inject
    @Named("taskService")
    TaskService taskService;

    /**
     * Execute on quarkus stop
     *
     * @param event shutdown event
     */
    public void onStop(@Observes ShutdownEvent event) {
        mainServiceThread.interrupt();
        taskService.stopTask();
    }
}
