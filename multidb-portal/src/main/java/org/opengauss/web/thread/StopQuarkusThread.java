/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.web.thread;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.opengauss.Main;
import org.opengauss.utils.ThreadUtils;

/**
 * Stop quarkus thread
 *
 * @since 2025/7/12
 */
@Named("stopQuarkusThread")
@ApplicationScoped
public class StopQuarkusThread extends Thread {
    public StopQuarkusThread() {
        super("Stop-Quarkus-Thread");
    }

    @Override
    public void run() {
        ThreadUtils.sleep(2000);
        Main.stopQuarkus();
    }
}
