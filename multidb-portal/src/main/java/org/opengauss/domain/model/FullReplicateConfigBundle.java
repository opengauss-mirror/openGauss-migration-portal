/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

import lombok.Data;

/**
 * oG_datasync_full_migration config file bundle
 *
 * @since 2025/7/2
 */
@Data
public class FullReplicateConfigBundle extends AbstractToolConfigBundle {
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
