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

package org.opengauss.migration.domain.dto;

import lombok.Data;
import org.opengauss.migration.domain.model.ConfigFile;

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
