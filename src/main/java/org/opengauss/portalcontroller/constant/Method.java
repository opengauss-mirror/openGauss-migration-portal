package org.opengauss.portalcontroller.constant;

import java.util.Arrays;
import java.util.List;

/**
 * Method constants
 *
 * @author ：liutong
 * @date ：Created in 2023/06/03
 * @since ：11
 */
public interface Method {
    /**
     * The interface Run.
     */
    interface Run {
        /**
         * The constant ZOOKEEPER.
         */
        String ZOOKEEPER = "runZookeeper";

        /**
         * The constant KAFKA.
         */
        String KAFKA = "runKafka";

        /**
         * The constant REGISTRY.
         */
        String REGISTRY = "runSchemaRegistry";

        /**
         * The constant CONNECT_SINK.
         */
        String CONNECT_SINK = "runKafkaConnectSink";

        /**
         * The constant CONNECT_SOURCE.
         */
        String CONNECT_SOURCE = "runKafkaConnectSource";

        /**
         * The constant REVERSE_CONNECT_SINK.
         */
        String REVERSE_CONNECT_SINK = "runReverseKafkaConnectSink";

        /**
         * The constant REVERSE_CONNECT_SOURCE.
         */
        String REVERSE_CONNECT_SOURCE = "runReverseKafkaConnectSource";

        /**
         * The constant CHECK_SOURCE.
         */
        String CHECK_SOURCE = "runDataCheckSource";

        /**
         * The constant CHECK_SINK.
         */
        String CHECK_SINK = "runDataCheckSink";

        /**
         * The constant CHECK.
         */
        String CHECK = "runDataCheck";
    }

    /**
     * The interface Name.
     */
    interface Name {
        /**
         * The constant ZOOKEEPER.
         */
        String ZOOKEEPER = "zookeeper";

        /**
         * The constant KAFKA.
         */
        String KAFKA = "kafka";

        /**
         * The constant REGISTRY.
         */
        String REGISTRY = "schema registry";

        /**
         * The constant CONNECT_SINK.
         */
        String CONNECT_SINK = "mysql to opengauss connector sink";

        /**
         * The constant CONNECT_SOURCE.
         */
        String CONNECT_SOURCE = "mysql to opengauss connector source";

        /**
         * The constant REVERSE_CONNECT_SINK.
         */
        String REVERSE_CONNECT_SINK = "opengauss to mysql connector sink";

        /**
         * The constant REVERSE_CONNECT_SOURCE.
         */
        String REVERSE_CONNECT_SOURCE = "opengauss to mysql connector source";

        /**
         * The constant CHECK_SOURCE.
         */
        String CHECK_SOURCE = "datacheck source";

        /**
         * The constant CHECK_SINK.
         */
        String CHECK_SINK = "datacheck sink";

        /**
         * The constant CHECK.
         */
        String CHECK = "datacheck";

        /**
         * The constant CONNECT_TYPE_LIST.
         */
        List<String> CONNECT_TYPE_LIST = Arrays.asList(CONNECT_SINK, CONNECT_SOURCE, REVERSE_CONNECT_SINK,
            REVERSE_CONNECT_SOURCE);
    }
}
