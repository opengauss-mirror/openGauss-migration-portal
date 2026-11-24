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

package org.opengauss.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Migration exit notifier, notifies host to do cleanup when migration exits
 *
 * @since 2026/10/10
 */
public class MigrationExitNotifier {
    private static final Logger LOGGER = LogManager.getLogger(MigrationExitNotifier.class);

    private final AtomicBoolean fired = new AtomicBoolean(false);
    private final List<Runnable> callbacks = new CopyOnWriteArrayList<>();

    /**
     * Register exit callback
     *
     * @param callback exit callback
     */
    public void registerCallback(Runnable callback) {
        callbacks.add(callback);
    }

    /**
     * Fire exit callbacks, only the first invocation takes effect
     */
    public void fireExit() {
        if (!fired.compareAndSet(false, true)) {
            return;
        }
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (Exception e) {
                LOGGER.error("Migration exit callback execute failed", e);
            }
        }
    }
}
