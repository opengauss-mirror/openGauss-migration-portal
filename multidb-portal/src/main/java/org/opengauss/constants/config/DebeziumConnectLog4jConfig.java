/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

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
