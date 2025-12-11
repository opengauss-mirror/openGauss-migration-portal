/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.tool.config;

import lombok.Data;
import org.opengauss.domain.model.ConfigFile;

/**
 * The config file bundle for the migration tool with only one config file
 *
 * @since 2025/7/2
 */
@Data
public class SingleConfigBundle extends AbstractToolConfigBundle {
    private ConfigFile configFile;

    @Override
    public void loadConfigMap() {
        configFile.loadConfigMap();
    }

    @Override
    public void saveConfigMap() {
        configFile.saveConfigMap();
    }

    @Override
    public void generateFile() {
        configFile.generateFile();
    }
}
