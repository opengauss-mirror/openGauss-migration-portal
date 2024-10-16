/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.utils;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * migration param utils
 *
 * @since 2024-10-11
 */
public class MigrationParamUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationParamUtils.class);

    /**
     * get kafka ip address in config
     *
     * @return String
     */
    public static String getKafkaIp() {
        String kafkaPort = PortalControl.toolsMigrationParametersTable.get(Parameter.Port.KAFKA);
        int colonIndex = kafkaPort.indexOf(":");
        if (colonIndex != -1) {
            return kafkaPort.substring(0, colonIndex);
        }
        LOGGER.error("Failed to parse the kafka ip address.");
        return "";
    }
}
