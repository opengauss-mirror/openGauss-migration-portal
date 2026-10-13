/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
