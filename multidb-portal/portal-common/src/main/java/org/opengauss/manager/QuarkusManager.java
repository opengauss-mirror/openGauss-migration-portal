/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.manager;

import io.quarkus.runtime.Quarkus;

/**
 * Quarkus manager
 *
 * @since 2026/4/1
 */
public class QuarkusManager {
    /**
     * Start Quarkus
     *
     * @param args start args
     */
    public static void startQuarkus(String[] args) {
        Quarkus.run(args);
    }

    /**
     * Stop Quarkus
     */
    public static void stopQuarkus() {
        Quarkus.asyncExit();
    }
}
