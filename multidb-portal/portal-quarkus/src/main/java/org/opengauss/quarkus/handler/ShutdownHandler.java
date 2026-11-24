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

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.opengauss.quarkus.thread.StartMigrationThread;
import org.opengauss.quarkus.service.TaskService;

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
