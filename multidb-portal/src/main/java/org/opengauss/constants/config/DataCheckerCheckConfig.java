/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * data checker check config
 *
 * @since 2025/5/8
 */
public class DataCheckerCheckConfig {
    /**
     * data check data path
     */
    public static final String DATA_CHECK_DATA_PATH = "data.check.data-path";

    /**
     * kafka bootstrap servers
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "spring.kafka.bootstrap-servers";

    /**
     * logging config file path
     */
    public static final String LOGGING_CONFIG = "logging.config";

    /**
     * check source process uri
     */
    public static final String CHECK_SOURCE_URI = "data.check.source-uri";

    /**
     * check sink process uri
     */
    public static final String CHECK_SINK_URI = "data.check.sink-uri";

    /**
     * check server port
     */
    public static final String SERVER_PORT = "server.port";

    private DataCheckerCheckConfig() {
    }
}
