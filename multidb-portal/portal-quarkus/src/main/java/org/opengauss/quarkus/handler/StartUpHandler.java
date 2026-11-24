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

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.opengauss.quarkus.thread.StartMigrationThread;

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
