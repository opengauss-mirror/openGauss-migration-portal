/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.tool.config;

import lombok.Data;
import org.opengauss.domain.model.ConfigFile;

/**
 * debezium config file bundle
 *
 * @since 2025/7/2
 */
@Data
public class DebeziumConfigBundle extends AbstractToolConfigBundle {
    private ConfigFile connectSinkConfigFile;
    private ConfigFile connectSourceConfigFile;
    private ConfigFile workerSinkConfigFile;
    private ConfigFile workerSourceConfigFile;
    private ConfigFile log4jSinkConfigFile;
    private ConfigFile log4jSourceConfigFile;

    @Override
    public void loadConfigMap() {
        connectSinkConfigFile.loadConfigMap();
        connectSourceConfigFile.loadConfigMap();
        workerSinkConfigFile.loadConfigMap();
        workerSourceConfigFile.loadConfigMap();
        log4jSinkConfigFile.loadConfigMap();
        log4jSourceConfigFile.loadConfigMap();
    }

    @Override
    public void saveConfigMap() {
        connectSinkConfigFile.saveConfigMap();
        connectSourceConfigFile.saveConfigMap();
        workerSinkConfigFile.saveConfigMap();
        workerSourceConfigFile.saveConfigMap();
        log4jSinkConfigFile.saveConfigMap();
        log4jSourceConfigFile.saveConfigMap();
    }

    @Override
    public void generateFile() {
        connectSinkConfigFile.generateFile();
        connectSourceConfigFile.generateFile();
        workerSinkConfigFile.generateFile();
        workerSourceConfigFile.generateFile();
        log4jSinkConfigFile.generateFile();
        log4jSourceConfigFile.generateFile();
    }
}
