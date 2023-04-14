package org.opengauss.portalcontroller.constant;

public interface Debezium {
    String PATH = "debezium.path";
    String PKG_PATH = "debezium.pkg.path";
    String CONFIG_PATH = "debezium.config.path";
    String LOG_PATH = "debezium.log.path";
    interface Zookeeper{
        String PATH = "zookeeper.path";
        String CONFIG_PATH = "zookeeper.config.path";
        String TMP_PATH = "zookeeper.tmp.path";
        String LOG_PATH = "zookeeper.log.path";
    }

    interface Kafka {
        String NAME = "kafka";
        String PATH = "kafka.path";
        String PKG_URL = "kafka.pkg.url";
        String PKG_NAME = "kafka.pkg.name";
        String CONFIG_PATH = "kafka.config.path";
        String TMP_PATH = "kafka.tmp.path";
        String LOG_PATH = "kafka.log.path";
    }

    interface Registry{
        String LOG_PATH = "registry.log.path";
        String CONFIG_PATH = "registry.config.path";
    }

    interface Confluent {
        String NAME = "confluent";
        String PATH = "confluent.path";
        String PKG_URL = "confluent.pkg.url";
        String PKG_NAME = "confluent.pkg.name";
        String CONFIG_PATH = "kafka.confluent.path";
    }
    interface Connector {
        String MYSQL_NAME = "connectorMysql";
        String OPENGAUSS_NAME = "connectorOpengauss";
        String PATH = "connector.path";
        String MYSQL_PATH = "connector.mysql.path";
        String OPENGAUSS_PATH = "connector.opengauss.path";
        String CONFIG_PATH = "connector.standalone.path";
        String MYSQL_PKG_URL = "connector.mysql.pkg.url";
        String MYSQL_PKG_NAME = "connector.mysql.pkg.name";
        String OPENGAUSS_PKG_URL = "connector.opengauss.pkg.url";
        String OPENGAUSS_PKG_NAME = "connector.opengauss.pkg.name";
        String LOG_PATTERN_PATH = "connector.log.pattern.path";
    }
    interface Source {
        String HOST = "database.hostname";
        String PORT = "database.port";
        String USER = "database.user";
        String PASSWORD = "database.password";
        String WHITELIST = "database.include.list";
        String NAME = "database.dbname";
        String CONNECTOR_PATH = "source.incremental.connector.path";
        String REVERSE_CONNECTOR_PATH = "source.reverse.connector.path";
        String INCREMENTAL_CONFIG_PATH = "source.incremental.config.path";
        String REVERSE_CONFIG_PATH = "source.reverse.config.path";
        String LOG_PATH = "source.log.path";
        String REVERSE_LOG_PATH = "source.reverse.log.path";
    }
    interface Sink {
        interface Mysql {
            String USER = "mysql.username";
            String PASSWORD = "mysql.password";
            String URL = "mysql.url";
            String PORT = "mysql.port";
            String NAME = "mysql.database";
        }

        interface Opengauss {
            String USER = "opengauss.username";
            String PASSWORD = "opengauss.password";
            String URL = "opengauss.url";
        }
        String SCHEMA_MAPPING = "schema.mappings";
        String CONNECTOR_PATH = "sink.incremental.connector.path";
        String REVERSE_CONNECTOR_PATH = "sink.reverse.connector.path";
        String INCREMENTAL_CONFIG_PATH = "sink.incremental.config.path";
        String REVERSE_CONFIG_PATH = "sink.reverse.config.path";
        String LOG_PATH = "sink.log.path";
        String REVERSE_LOG_PATH = "sink.reverse.log.path";
    }
}
