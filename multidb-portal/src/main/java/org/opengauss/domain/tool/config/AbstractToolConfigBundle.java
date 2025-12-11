/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.tool.config;

/**
 * Abstract tool config bundle
 *
 * @since 2025/7/2
 */
public abstract class AbstractToolConfigBundle {
    /**
     * load config map from config file
     */
    public abstract void loadConfigMap();

    /**
     * save config map to config file
     */
    public abstract void saveConfigMap();

    /**
     * generate config file when create task
     */
    public abstract void generateFile();
}
