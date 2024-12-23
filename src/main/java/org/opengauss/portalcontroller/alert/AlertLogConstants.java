/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import java.util.List;

/**
 * alert log constants
 *
 * @since 2024/12/6
 */
public interface AlertLogConstants {
    /**
     * kafka topic model
     */
    String KAFKA_TOPIC_MODEL = "alert_log_%s";

    /**
     * alert file home path model
     */
    String ALERT_FILE_HOME_PATH_MODEL = "%s/workspace/%s/status/alert/";

    /**
     * register code causeCn prefix
     */
    String CODE_CAUSE_CN_PREFIX = "<CODE:causeCn>";

    /**
     * register code causeEn prefix
     */
    String CODE_CAUSE_EN_PREFIX = "<CODE:causeEn>";

    /**
     * alert file name model
     */
    String ALERT_FILE_NAME_MODEL = "alert_%d.txt";

    /**
     * alert log entity json string separator in alert file
     */
    String OBJECT_SEPARATOR = "<<<END_OF_OBJECT>>>";

    /**
     * get migration tool name by source name
     *
     * @param source sourceName
     * @return String migration tool name
     */
    static String getMigrationToolBySource(String source) {
        if (source.equals(AlertLogSources.PORTAL)) {
            return AlertLogConstants.MigrationTools.PORTAL;
        } else if (source.equals(AlertLogSources.CHAMELEON)) {
            return AlertLogConstants.MigrationTools.CHAMELEON;
        } else if (source.startsWith("data_check_")) {
            return AlertLogConstants.MigrationTools.DATA_CHECKER;
        } else {
            return AlertLogConstants.MigrationTools.DEBEZIUM;
        }
    }

    /**
     * parameters related to alert log configuration
     */
    interface Params {
        /**
         * configuration key to enable or disable alert log collection
         */
        String ENABLE_ALERT_LOG_COLLECTION = "enable.alert.log.collection";

        /**
         * configuration key for Kafka bootstrap servers
         */
        String KAFKA_SEVER = "kafka.bootstrapServers";
    }

    /**
     * migration tool names
     */
    interface MigrationTools {
        /**
         * migration tool portal
         */
        String PORTAL = "portal";

        /**
         * migration tool chameleon
         */
        String CHAMELEON = "chameleon";

        /**
         * migration tool datachecker
         */
        String DATA_CHECKER = "datachecker";

        /**
         * migration tool debezium
         */
        String DEBEZIUM = "debezium";

        /**
         * migration tool name list
         */
        List<String> MIGRATION_TOOL_LIST = List.of(PORTAL, CHAMELEON, DATA_CHECKER, DEBEZIUM);
    }

    /**
     * alert log source names
     */
    interface AlertLogSources {
        /**
         * alert log source portal
         */
        String PORTAL = "portal";

        /**
         * alert log source chameleon
         */
        String CHAMELEON = "chameleon";

        /**
         * alert log source data_checker check
         */
        String DATA_CHECK_CHECK = "data_check_check";

        /**
         * alert log source data_checker source
         */
        String DATA_CHECK_SOURCE = "data_check_source";

        /**
         * alert log source data_checker sink
         */
        String DATA_CHECK_SINK = "data_check_sink";

        /**
         * alert log source incremental connect source
         */
        String CONNECT_SOURCE = "connect_source";

        /**
         * alert log source incremental connect sink
         */
        String CONNECT_SINK = "connect_sink";

        /**
         * alert log source reverse connect source
         */
        String REVERSE_CONNECT_SOURCE = "reverse_connect_source";

        /**
         * alert log source reverse connect sink
         */
        String REVERSE_CONNECT_SINK = "reverse_connect_sink";

        /**
         * alert log sources list
         */
        List<String> ALERT_LOG_SOURCE_LIST = List.of(PORTAL, CHAMELEON, DATA_CHECK_CHECK,
                DATA_CHECK_SOURCE, DATA_CHECK_SINK, CONNECT_SOURCE, CONNECT_SINK,
                REVERSE_CONNECT_SOURCE, REVERSE_CONNECT_SINK);
    }
}
