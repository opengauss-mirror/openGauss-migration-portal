/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opengauss.portalcontroller.constant.ToolsParamsLog;

/**
 * ToolsConfigEnum
 *
 * @author: www
 * @date: 2023/11/28 15:18
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
@AllArgsConstructor
@Getter
public enum ToolsConfigEnum {
    CHAMELEON_CONFIG(1, "config.yml",
            ToolsParamsLog.CHAMELEON_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.CHAMELEON_LOAD_CONFIG + ToolsParamsLog.END),
    DATA_CHECK_APPLICATION(2, "application.yml",
            ToolsParamsLog.DATA_CHECK_APPLICATION_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DATA_CHECK_APPLICATION_LOAD_CONFIG + ToolsParamsLog.END),
    DATA_CHECK_APPLICATION_SINK(3, "application-sink.yml",
            ToolsParamsLog.DATA_CHECK_APPLICATION_SINK_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DATA_CHECK_APPLICATION_SINK_LOAD_CONFIG + ToolsParamsLog.END),
    DATA_CHECK_APPLICATION_SOURCE(4, "application-source.yml",
            ToolsParamsLog.DATA_CHECK_APPLICATION__SOURCE_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DATA_CHECK_APPLICATION__SOURCE_LOAD_CONFIG + ToolsParamsLog.END),
    DEBEZIUM_MYSQL_SINK(5, "mysql-sink.properties",
            ToolsParamsLog.DEBEZIUM_MYSQL_SINK_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DEBEZIUM_MYSQL_SINK_LOAD_CONFIG + ToolsParamsLog.END),
    DEBEZIUM_MYSQL_SOURCE(6, "mysql-source.properties",
            ToolsParamsLog.DEBEZIUM_MYSQL_SOURCE_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DEBEZIUM_MYSQL_SOURCE_LOAD_CONFIG + ToolsParamsLog.END),
    DEBEZIUM_OPENGAUSS_SINK(7, "opengauss-sink.properties",
            ToolsParamsLog.DEBEZIUM_OPENGAUSS_SINK_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DEBEZIUM_OPENGAUSS_SINK_LOAD_CONFIG + ToolsParamsLog.END),
    DEBEZIUM_OPENGAUSS_SOURCE(8, "opengauss-source.properties",
            ToolsParamsLog.DEBEZIUM_OPENGAUSS_SOURCE_LOAD_CONFIG + ToolsParamsLog.START,
            ToolsParamsLog.DEBEZIUM_OPENGAUSS_SOURCE_LOAD_CONFIG + ToolsParamsLog.END),
    PORTAL_MIGRATION(9, "migrationConfig.properties",
            ToolsParamsLog.PORTAL_MIGRATION + ToolsParamsLog.START,
            ToolsParamsLog.PORTAL_MIGRATION + ToolsParamsLog.END);
    private Integer type;
    private String configName;
    private String startFromLog;
    private String endStrFromLog;
}
