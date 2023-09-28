/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
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

package org.opengauss.portalcontroller.constant;

/**
 * The interface Check.
 *
 * @author ：liutong
 * @date ：Created in 2023/07/29
 * @since ：11
 *
 */
public interface Check {
    /**
     * The constant PKG_URL.
     */
    String PKG_URL = "datacheck.pkg.url";

    /**
     * The constant INSTALL_PATH.
     */
    String INSTALL_PATH = "datacheck.install.path";

    /**
     * The constant PATH.
     */
    String PATH = "datacheck.path";

    /**
     * The constant PKG_PATH.
     */
    String PKG_PATH = "datacheck.pkg.path";

    /**
     * The constant PKG_NAME.
     */
    String PKG_NAME = "datacheck.pkg.name";

    /**
     * The constant PKG_UNZIP_SPACE.
     */
    String PKG_UNZIP_SPACE = "100MB";

    /**
     * The constant CONFIG_PATH.
     */
    String CONFIG_PATH = "datacheck.config.path";

    /**
     * The constant LOG_FOLDER.
     */
    String LOG_FOLDER = "datacheck.log.folder";

    /**
     * The constant LOG_PATH.
     */
    String LOG_PATH = "datacheck.log.path";

    /**
     * The constant LOG_PATTERN_PATH.
     */
    String LOG_PATTERN_PATH = "datacheck.log.pattern.path";

    /**
     * The constant EXTRACT_NAME.
     */
    String EXTRACT_NAME = "datacheck.extract.jar.name";

    /**
     * The constant CHECK_NAME.
     */
    String CHECK_NAME = "datacheck.check.jar.name";

    /**
     * The constant FULL_EXTRACT_SOURCE_JVM.
     */
    String FULL_EXTRACT_SOURCE_JVM = "full.check.extract.source.jvm";

    /**
     * The constant FULL_EXTRACT_SINK_JVM.
     */
    String FULL_EXTRACT_SINK_JVM = "full.check.extract.sink.jvm";

    /**
     * The constant FULL_CHECK_JVM.
     */
    String FULL_CHECK_JVM = "full.check.jvm";

    /**
     * The constant INCREMENTAL_EXTRACT_SOURCE_JVM.
     */
    String INCREMENTAL_EXTRACT_SOURCE_JVM = "incremental.check.extract.source.jvm";

    /**
     * The constant INCREMENTAL_EXTRACT_SINK_JVM.
     */
    String INCREMENTAL_EXTRACT_SINK_JVM = "incremental.check.extract.sink.jvm";

    /**
     * The constant INCREMENTAL_CHECK_JVM.
     */
    String INCREMENTAL_CHECK_JVM = "incremental.check.jvm";

    /**
     * The constant DROP_LOGICAL_SLOT.
     */
    String DROP_LOGICAL_SLOT = "drop.logical.slot.on.stop";

    /**
     * The interface Parameters.
     */
    interface Parameters {
        /**
         * The constant SCHEMA.
         */
        String SCHEMA = "spring.extract.schema";

        /**
         * The constant URL.
         */
        String URL = "spring.datasource.url";

        /**
         * The constant USER_NAME.
         */
        String USER_NAME = "spring.datasource.username";

        /**
         * The constant PASSWORD.
         */
        String PASSWORD = "spring.datasource.password";

        /**
         * The constant QUERY_DOP.
         */
        String QUERY_DOP = "spring.extract.query-dop";

        /**
         * The constant MIN_IDLE.
         */
        String MIN_IDLE = "spring.datasource.druid.min-idle";

        /**
         * The constant MAX_ACTIVE.
         */
        String MAX_ACTIVE = "spring.datasource.druid.max-active";

        /**
         * The constant INITIAL_SIZE.
         */
        String INITIAL_SIZE = "spring.datasource.druid.initial-size";

        /**
         * The constant TIME_PERIOD.
         */
        String TIME_PERIOD = "spring.extract.debezium-time-period";

        /**
         * The constant NUM_PERIOD.
         */
        String NUM_PERIOD = "spring.extract.debezium-num-period";
    }

    /**
     * The interface Sink.
     */
    interface Sink {
        /**
         * The constant QUERY_DOP.
         */
        String QUERY_DOP = "sink.query-dop";

        /**
         * The constant MIN_IDLE.
         */
        String MIN_IDLE = "sink.minIdle";

        /**
         * The constant MAX_ACTIVE.
         */
        String MAX_ACTIVE = "sink.maxActive";

        /**
         * The constant INITIAL_SIZE.
         */
        String INITIAL_SIZE = "sink.initialSize";

        /**
         * The constant TIME_PERIOD.
         */
        String TIME_PERIOD = "sink.debezium-time-period";

        /**
         * The constant NUM_PERIOD.
         */
        String NUM_PERIOD = "sink.debezium-num-period";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "datacheck.sink.config.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "datacheck.sink.log.path";

        /**
         * The constant LOG_PATTERN_PATH.
         */
        String LOG_PATTERN_PATH = "datacheck.sink.log.pattern.path";
    }

    /**
     * The interface Source.
     */
    interface Source {
        /**
         * The constant QUERY_DOP.
         */
        String QUERY_DOP = "source.query-dop";

        /**
         * The constant MIN_IDLE.
         */
        String MIN_IDLE = "source.minIdle";

        /**
         * The constant MAX_ACTIVE.
         */
        String MAX_ACTIVE = "source.maxActive";

        /**
         * The constant INITIAL_SIZE.
         */
        String INITIAL_SIZE = "source.initialSize";

        /**
         * The constant TIME_PERIOD.
         */
        String TIME_PERIOD = "source.debezium-time-period";

        /**
         * The constant NUM_PERIOD.
         */
        String NUM_PERIOD = "source.debezium-num-period";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "datacheck.source.config.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "datacheck.source.log.path";

        /**
         * The constant LOG_PATTERN_PATH.
         */
        String LOG_PATTERN_PATH = "datacheck.source.log.pattern.path";
    }

    /**
     * The interface Rules.
     */
    interface Rules {
        /**
         * The constant ENABLE.
         */
        String ENABLE = "rules.enable";

        /**
         * The interface Table.
         */
        interface Table {
            /**
             * The constant AMOUNT.
             */
            String AMOUNT = "rules.table";

            /**
             * The constant NAME.
             */
            String NAME = "rules.table.name";

            /**
             * The constant TEXT.
             */
            String TEXT = "rules.table.text";
        }

        /**
         * The interface Row.
         */
        interface Row {
            /**
             * The constant AMOUNT.
             */
            String AMOUNT = "rules.row";

            /**
             * The constant NAME.
             */
            String NAME = "rules.row.name";

            /**
             * The constant TEXT.
             */
            String TEXT = "rules.row.text";
        }

        /**
         * The interface Column.
         */
        interface Column {
            /**
             * The constant AMOUNT.
             */
            String AMOUNT = "rules.column";

            /**
             * The constant NAME.
             */
            String NAME = "rules.column.name";

            /**
             * The constant TEXT.
             */
            String TEXT = "rules.column.text";

            /**
             * The constant ATTRIBUTE.
             */
            String ATTRIBUTE = "rules.column.attribute";
        }
    }

    /**
     * The interface Result.
     */
    interface Result {
        /**
         * The constant FULL.
         */
        String FULL = "full.migration.datacheck.result";

        /**
         * The constant FULL_CURRENT.
         */
        String FULL_CURRENT = "full.migration.datacheck.current.result";

        /**
         * The constant INCREMENTAL.
         */
        String INCREMENTAL = "incremental.migration.datacheck.result";

        /**
         * The constant REVERSE.
         */
        String REVERSE = "reverse.migration.datacheck.result";
    }

    /**
     * CheckPortalStatus constance
     */
    interface CheckPortalStatus {
        /**
         * status ok
         */
        String CHECK_STATUS_OK = "ok";

        /**
         * chameleon is not ready...
         */
        String CHECK_STATUS_CHAMELEON_ERR = "chameleon is not ready...";

        /**
         * kafka process is not ready
         */
        String CHECK_STATUS_KAFKA_ERR = "kafka process is not ready";

        /**
         * file is not exist...
         */
        String CHECK_STATUS_FILE_NOT_EXIST_ERR = "file is not exist...";

        /**
         * prefix check portal status:
         */
        String CHECK_STATUS_PREFIX = "check portal status:";
    }
}
