package org.opengauss.portalcontroller.constant;

public interface Check {
    String NAME = "datacheck";
    String PKG_URL = "datacheck.pkg.url";
    String INSTALL_PATH = "datacheck.install.path";
    String PATH = "datacheck.path";
    String PKG_PATH = "datacheck.pkg.path";
    String PKG_NAME = "datacheck.pkg.name";
    String CONFIG_PATH = "datacheck.config.path";
    String LOG_FOLDER = "datacheck.log.folder";
    String LOG_PATH = "datacheck.log.path";
    String LOG_PATTERN_PATH = "datacheck.log.pattern.path";
    String EXTRACT_NAME = "datacheck.extract.jar.name";
    String CHECK_NAME = "datacheck.check.jar.name";
    String FULL_EXTRACT_SOURCE_JVM = "full.check.extract.source.jvm";
    String FULL_EXTRACT_SINK_JVM = "full.check.extract.sink.jvm";
    String FULL_CHECK_JVM = "full.check.jvm";
    String INCREMENTAL_EXTRACT_SOURCE_JVM = "incremental.check.extract.source.jvm";
    String INCREMENTAL_EXTRACT_SINK_JVM = "incremental.check.extract.sink.jvm";
    String INCREMENTAL_CHECK_JVM = "incremental.check.jvm";
    String DROP_LOGICAL_SLOT = "drop.logical.slot.on.stop";

    interface Parameters {
        String SCHEMA = "spring.extract.schema";
        String URL = "spring.datasource.druid.dataSourceOne.url";
        String USER_NAME = "spring.datasource.druid.dataSourceOne.username";
        String PASSWORD = "spring.datasource.druid.dataSourceOne.password";
        String QUERY_DOP = "spring.extract.query-dop";
        String MIN_IDLE = "spring.datasource.druid.dataSourceOne.minIdle";
        String MAX_ACTIVE = "spring.datasource.druid.dataSourceOne.maxActive";
        String INITIAL_SIZE = "spring.datasource.druid.dataSourceOne.initialSize";
        String TIME_PERIOD = "spring.extract.debezium-time-period";
        String NUM_PERIOD = "spring.extract.debezium-num-period";
    }

    interface Sink {
        String QUERY_DOP = "sink.query-dop";
        String MIN_IDLE = "sink.minIdle";
        String MAX_ACTIVE = "sink.maxActive";
        String INITIAL_SIZE = "sink.initialSize";
        String TIME_PERIOD = "sink.debezium-time-period";
        String NUM_PERIOD = "sink.debezium-num-period";
        String CONFIG_PATH = "datacheck.sink.config.path";
        String LOG_PATH = "datacheck.sink.log.path";
        String LOG_PATTERN_PATH = "datacheck.sink.log.pattern.path";
    }

    interface Source {
        String QUERY_DOP = "source.query-dop";
        String MIN_IDLE = "source.minIdle";
        String MAX_ACTIVE = "source.maxActive";
        String INITIAL_SIZE = "source.initialSize";
        String TIME_PERIOD = "source.debezium-time-period";
        String NUM_PERIOD = "source.debezium-num-period";
        String CONFIG_PATH = "datacheck.source.config.path";
        String LOG_PATH = "datacheck.source.log.path";
        String LOG_PATTERN_PATH = "datacheck.source.log.pattern.path";
    }

    interface Rules {
        String ENABLE = "rules.enable";

        interface Table {
            String AMOUNT = "rules.table";
            String NAME = "rules.table.name";
            String TEXT = "rules.table.text";
        }

        interface Row {
            String AMOUNT = "rules.row";
            String NAME = "rules.row.name";
            String TEXT = "rules.row.text";
        }

        interface Column {
            String AMOUNT = "rules.column";
            String NAME = "rules.column.name";
            String TEXT = "rules.column.text";

            String ATTRIBUTE = "rules.column.attribute";
        }
    }

    interface Result {
        String FULL = "full.migration.datacheck.result";
        String FULL_CURRENT = "full.migration.datacheck.current.result";
        String INCREMENTAL = "incremental.migration.datacheck.result";
        String REVERSE = "reverse.migration.datacheck.result";
    }
}
