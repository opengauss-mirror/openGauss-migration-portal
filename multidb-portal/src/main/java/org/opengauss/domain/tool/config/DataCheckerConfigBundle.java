/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.tool.config;

import lombok.Data;
import org.opengauss.domain.model.ConfigFile;

/**
 * data-checker config file bundle
 *
 * @since 2025/7/2
 */
@Data
public class DataCheckerConfigBundle extends AbstractToolConfigBundle {
    private ConfigFile checkConfigFile;
    private ConfigFile sinkConfigFile;
    private ConfigFile sourceConfigFile;
    private ConfigFile log4j2ConfigFile;

    @Override
    public void loadConfigMap() {
        checkConfigFile.loadConfigMap();
        sinkConfigFile.loadConfigMap();
        sourceConfigFile.loadConfigMap();
    }

    @Override
    public void saveConfigMap() {
        checkConfigFile.saveConfigMap();
        sinkConfigFile.saveConfigMap();
        sourceConfigFile.saveConfigMap();
        log4j2ConfigFile.saveConfigMap();
    }

    @Override
    public void generateFile() {
        checkConfigFile.generateFile();
        sinkConfigFile.generateFile();
        sourceConfigFile.generateFile();
        log4j2ConfigFile.generateFile();
    }
}
