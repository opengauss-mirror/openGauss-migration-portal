/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.handler;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.opengauss.web.thread.StartMigrationThread;

/**
 * Startup handler
 *
 * @since 2025/3/26
 */
@ApplicationScoped
public class StartUpHandler {
    private static final Logger LOGGER = LogManager.getLogger(StartUpHandler.class);

    @Inject
    @Named("startMigrationThread")
    StartMigrationThread startMigrationThread;

    @ConfigProperty(name = "quarkus.http.port")
    int port;

    /**
     * Execute on quarkus start
     *
     * @param event startup event
     */
    public void onStart(@Observes StartupEvent event) {
        LOGGER.info("Application has started, listening on port {}", port);
        startMigrationThread.start();
    }
}
