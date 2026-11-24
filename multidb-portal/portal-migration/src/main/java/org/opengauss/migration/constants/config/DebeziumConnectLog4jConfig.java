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

package org.opengauss.migration.constants.config;

/**
 * debezium connect log4j config
 *
 * @since 2025/5/19
 */
public class DebeziumConnectLog4jConfig {
    /**
     * connect appender file
     */
    public static final String CONNECT_APPENDER_FILE = "log4j.appender.connectAppender.File";

    /**
     * kafka error logger
     */
    public static final String KAFKA_ERROR_LOGGER = "log4j.logger.org.apache.kafka";

    /**
     * kafka error appender
     */
    public static final String KAFKA_ERROR_APPENDER = "log4j.appender.kafkaErrorAppender";

    /**
     * kafka error appender file
     */
    public static final String KAFKA_ERROR_APPENDER_FILE = "log4j.appender.kafkaErrorAppender.File";

    /**
     * kafka error appender layout
     */
    public static final String KAFKA_ERROR_APPENDER_LAYOUT = "log4j.appender.kafkaErrorAppender.layout";

    /**
     * kafka error appender layout conversion pattern
     */
    public static final String KAFKA_ERROR_APPENDER_LAYOUT_CONVERSION_PATTERN =
            "log4j.appender.kafkaErrorAppender.layout.ConversionPattern";

    private DebeziumConnectLog4jConfig() {
    }
}
