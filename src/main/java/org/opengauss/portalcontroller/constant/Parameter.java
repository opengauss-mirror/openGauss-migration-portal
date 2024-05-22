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
 * Parameter constants
 *
 * @author ：liutong
 * @date ：Created in 2023/06/03
 * @since ：11
 */
public interface Parameter {
    /**
     * The constant PATH.
     */
    String PATH = "path";

    /**
     * The constant PKG_URL.
     */
    String PKG_URL = "pkgUrl";

    /**
     * The constant PKG_PATH.
     */
    String PKG_PATH = "pkgPath";

    /**
     * The constant PKG_NAME.
     */
    String PKG_NAME = "pkgName";

    /**
     * The constant PKG_UNZIP_SPACE.
     */
    String PKG_UNZIP_SPACE = "pkgUnzipSpace";

    /**
     * The constant INSTALL_PATH.
     */
    String INSTALL_PATH = "installPath";

    /**
     * The constant PORTAL_NAME.
     */
    String PORTAL_NAME = "portalControl-6.0.0-exec.jar";

    /**
     * The constant INSTALL_ALL_MIGRATION_TOOLS.
     */
    String INSTALL_ALL_MIGRATION_TOOLS = "Install all migration tools";

    /**
     * The constant CHECK.
     */
    String CHECK = "datacheck";

    /**
     * The constant CHECK_FULL.
     */
    String CHECK_FULL = "Full migration datacheck";

    /**
     * The constant CHECK_INCREMENTAL.
     */
    String CHECK_INCREMENTAL = "Incremental migration datacheck";

    /**
     * The constant CHECK_REVERSE.
     */
    String CHECK_REVERSE = "Reverse migration datacheck";

    /**
     * The constant ERROR_PATH.
     */
    String ERROR_PATH = "error.path";

    /**
     * The constant INPUT_ORDER_PATH.
     */
    String INPUT_ORDER_PATH = "input.order.path";

    /**
     * The constant TOOLS_VERSION.
     */
    String TOOL_VERSION = "tools.version";

    /**
     * The constant TOOLS_VERSION.
     */
    String SYSTEM_NAME = "system.name";

    /**
     * The constant TOOLS_VERSION.
     */
    String SYSTEM_ARCH = "system.arch";

    /**
     * The interface Incremental status.
     */
    interface IncrementalStatus {
        /**
         * The constant REPLAYED_COUNT.
         */
        String REPLAYED_COUNT = "replayedCount";

        /**
         * The constant OVER_ALL_PIPE.
         */
        String OVER_ALL_PIPE = "overallPipe";

        /**
         * The constant SPEED.
         */
        String SPEED = "speed";

        /**
         * The constant FAIL.
         */
        String FAIL = "failCount";

        /**
         * The constant SUCCESS.
         */
        String SUCCESS = "successCount";

        /**
         * The constant SKIPPED.
         */
        String SKIP = "skippedCount";

        /**
         * The constant SKIPPED_EXCLUDE_EVENT_COUNT.
         */
        String SKIPPED_EXCLUDE_EVENT_COUNT = "skippedExcludeEventCount";

        /**
         * The constant SKIPPED_EXCLUDE_COUNT.
         */
        String SKIPPED_EXCLUDE_COUNT = "skippedExcludeCount";
    }

    /**
     * The interface Port.
     */
    interface Port {
        /**
         * The constant ZOOKEEPER.
         */
        String ZOOKEEPER = "zookeeper.port";

        /**
         * The constant KAFKA.
         */
        String KAFKA = "kafka.port";

        /**
         * The constant SCHEMA_REGISTRY.
         */
        String SCHEMA_REGISTRY = "confluent.port";
    }

    /**
     * ThirdPartySoftwareInstanceParam
     *
     * @author: www
     * @date: 2023/11/28 12:06
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    interface ThirdPartySoftwareInstanceParam {
        /**
         * MigrationThirdPartySoftwareInstanceConfig
         */
        String THIRD_PARTY_SOFTWARE_INSTANCE_PARAM = "MigrationThirdPartySoftwareInstanceConfig";
    }
}
