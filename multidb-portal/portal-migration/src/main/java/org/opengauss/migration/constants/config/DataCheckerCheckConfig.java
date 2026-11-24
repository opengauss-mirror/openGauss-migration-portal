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
