/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Portal exception handler
 *
 * @since 2025/4/19
 */
public class PortalExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LogManager.getLogger(PortalExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        String errorMessage = String.format("thread %s occur exception: ", t.getName());
        LOGGER.error(errorMessage, e);
    }
}
