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
 * The interface Debezium.
 *
 * @author ：liutong
 * @date ：Created in 2023/07/29
 * @since ：11
 */
public interface Debezium {
    /**
     * The constant PATH.
     */
    String PATH = "debezium.path";

    /**
     * The constant PKG_PATH.
     */
    String PKG_PATH = "debezium.pkg.path";

    /**
     * The constant CONFIG_PATH.
     */
    String CONFIG_PATH = "debezium.config.path";

    /**
     * The constant LOG_PATH.
     */
    String LOG_PATH = "debezium.log.path";

    /**
     * The interface Zookeeper.
     */
    interface Zookeeper {
        /**
         * The constant PATH.
         */
        String PATH = "zookeeper.path";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "zookeeper.config.path";

        /**
         * The constant TMP_PATH.
         */
        String TMP_PATH = "zookeeper.tmp.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "zookeeper.log.path";
    }

    /**
     * The interface Kafka.
     */
    interface Kafka {
        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "kafka.config.path";

        /**
         * The constant TMP_PATH.
         */
        String TMP_PATH = "kafka.tmp.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "kafka.log.path";
    }

    /**
     * The interface Registry.
     */
    interface Registry {
        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "registry.log.path";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "registry.config.path";
    }

    /**
     * The interface Confluent.
     */
    interface Confluent {
        /**
         * The constant NAME.
         */
        String NAME = "confluent";

        /**
         * unzip dir name
         */
        String DIR_NAME = "confluent-5.5.1";

        /**
         * install path
         */
        String INSTALL_PATH = "confluent.install.path";

        /**
         * The constant PATH.
         */
        String PATH = "confluent.path";

        /**
         * The constant PKG_URL.
         */
        String PKG_URL = "confluent.pkg.url";

        /**
         * The constant PKG_NAME.
         */
        String PKG_NAME = "confluent.pkg.name";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "kafka.confluent.path";

        /**
         * The constant PKG_UNZIP_SPACE.
         */
        String PKG_UNZIP_SPACE = "500MB";
    }

    /**
     * The interface Connector.
     */
    interface Connector {
        /**
         * The constant MYSQL_NAME.
         */
        String MYSQL_NAME = "connectorMysql";

        /**
         * The constant OPENGAUSS_NAME.
         */
        String OPENGAUSS_NAME = "connectorOpengauss";

        /**
         * The constant PATH.
         */
        String PATH = "connector.path";

        /**
         * The constant MYSQL_PATH.
         */
        String MYSQL_PATH = "connector.mysql.path";

        /**
         * The constant OPENGAUSS_PATH.
         */
        String OPENGAUSS_PATH = "connector.opengauss.path";

        /**
         * The constant CONFIG_PATH.
         */
        String CONFIG_PATH = "connector.standalone.path";

        /**
         * The constant MYSQL_PKG_URL.
         */
        String MYSQL_PKG_URL = "connector.mysql.pkg.url";

        /**
         * The constant MYSQL_PKG_NAME.
         */
        String MYSQL_PKG_NAME = "connector.mysql.pkg.name";

        /**
         * The constant MYSQL_JAR_NAME.
         */
        String MYSQL_JAR_NAME = "debezium-connector-mysql-1.8.1.Final.jar";

        /**
         * The constant OPENGAUSS_PKG_URL.
         */
        String OPENGAUSS_PKG_URL = "connector.opengauss.pkg.url";

        /**
         * The constant OPENGAUSS_PKG_NAME.
         */
        String OPENGAUSS_PKG_NAME = "connector.opengauss.pkg.name";

        /**
         * The constant OPENGAUSS_JAR_NAME.
         */
        String OPENGAUSS_JAR_NAME = "debezium-connector-opengauss-1.8.1.Final.jar";

        /**
         * The constant LOG_PATTERN_PATH.
         */
        String LOG_PATTERN_PATH = "connector.log.pattern.path";

        /**
         * The constant PKG_UNZIP_SPACE.
         */
        String PKG_UNZIP_SPACE = "20MB";
    }

    /**
     * The interface Source.
     */
    interface Source {
        /**
         * The constant HOST.
         */
        String HOST = "database.hostname";

        /**
         * The constant PORT.
         */
        String PORT = "database.port";

        /**
         * The constant USER.
         */
        String USER = "database.user";

        /**
         * The constant PASSWORD.
         */
        String PASSWORD = "database.password";

        /**
         * The constant SERVER_ID.
         */
        String SERVER_ID = "database.server.id";

        /**
         * The constant WHITELIST.
         */
        String WHITELIST = "database.include.list";

        /**
         * The constant TABLELIST.
         */
        String TABLELIST = "table.include.list";

        /**
         * The constant NAME.
         */
        String NAME = "database.dbname";

        /**
         * The constant CONNECTOR_PATH.
         */
        String CONNECTOR_PATH = "source.incremental.connector.path";

        /**
         * The constant REVERSE_CONNECTOR_PATH.
         */
        String REVERSE_CONNECTOR_PATH = "source.reverse.connector.path";

        /**
         * The constant INCREMENTAL_CONFIG_PATH.
         */
        String INCREMENTAL_CONFIG_PATH = "source.incremental.config.path";

        /**
         * The constant REVERSE_CONFIG_PATH.
         */
        String REVERSE_CONFIG_PATH = "source.reverse.config.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "source.log.path";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String REVERSE_LOG_PATH = "source.reverse.log.path";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String INCREMENTAL_SOURCE_NUMA_PARAMS = "incremental.source.numa.params";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String REVERSE_SOURCE_NUMA_PARAMS = "reverse.source.numa.params";
    }

    /**
     * The interface Sink.
     */
    interface Sink {
        /**
         * The constant SCHEMA_MAPPING.
         */
        String SCHEMA_MAPPING = "schema.mappings";

        /**
         * The constant TABLELIST.
         */
        String TABLELIST = "table.include.list";

        /**
         * The constant CONNECTOR_PATH.
         */
        String CONNECTOR_PATH = "sink.incremental.connector.path";

        /**
         * The constant REVERSE_CONNECTOR_PATH.
         */
        String REVERSE_CONNECTOR_PATH = "sink.reverse.connector.path";

        /**
         * The constant INCREMENTAL_CONFIG_PATH.
         */
        String INCREMENTAL_CONFIG_PATH = "sink.incremental.config.path";

        /**
         * The constant REVERSE_CONFIG_PATH.
         */
        String REVERSE_CONFIG_PATH = "sink.reverse.config.path";

        /**
         * The constant LOG_PATH.
         */
        String LOG_PATH = "sink.log.path";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String REVERSE_LOG_PATH = "sink.reverse.log.path";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String REVERSE_SINK_NUMA_PARAMS = "reverse.sink.numa.params";

        /**
         * The constant REVERSE_LOG_PATH.
         */
        String INCREMENTAL_SINK_NUMA_PARAMS = "incremental.sink.numa.params";

        /**
         * The interface Mysql.
         */
        interface Mysql {
            /**
             * The constant USER.
             */
            String USER = "database.username";

            /**
             * The constant PASSWORD.
             */
            String PASSWORD = "database.password";

            /**
             * The constant URL.
             */
            String URL = "database.ip";

            /**
             * The constant PORT.
             */
            String PORT = "database.port";

            /**
             * The constant NAME.
             */
            String NAME = "database.name";
        }

        /**
         * The interface Opengauss.
         */
        interface Opengauss {
            /**
             * The constant USER.
             */
            String USER = "opengauss.username";

            /**
             * The constant PASSWORD.
             */
            String PASSWORD = "opengauss.password";

            /**
             * The constant URL.
             */
            String URL = "opengauss.url";
        }
    }
}
