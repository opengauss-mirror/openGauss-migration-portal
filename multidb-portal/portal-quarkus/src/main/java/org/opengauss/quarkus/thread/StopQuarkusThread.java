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

package org.opengauss.quarkus.thread;

import io.quarkus.runtime.Quarkus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import org.opengauss.migration.utils.ThreadUtils;

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
        Quarkus.asyncExit();
    }
}
