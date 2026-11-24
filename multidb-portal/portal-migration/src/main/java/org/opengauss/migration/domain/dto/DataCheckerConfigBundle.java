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
