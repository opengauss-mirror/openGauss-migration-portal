package org.opengauss.portalcontroller.constant;

public interface Chameleon {
    String VENV_PATH = "chameleon.venv.path";
    String PATH = "chameleon.path";
    String PKG_PATH = "chameleon.pkg.path";
    String PKG_NAME = "chameleon.pkg.name";
    String PKG_URL = "chameleon.pkg.url";
    String TMP_PATH = "chameleon.tmp.path";
    String LOG_PATH = "chameleon.log.path";
    String CONFIG_PATH = "chameleon.config.path";
    String RUNNABLE_FILE_PATH = "chameleon.runnable.file.path";
    interface Parameters{
        interface Mysql{
            String HOST = "sources.mysql.db_conn.host";
            String PORT = "sources.mysql.db_conn.port";
            String USER = "sources.mysql.db_conn.user";
            String PASSWORD = "sources.mysql.db_conn.password";
            String NAME = "sources.mysql.db_conn.database";
            String MAPPING = "sources.mysql.schema_mappings";
        }
        interface Opengauss{
            String HOST = "pg_conn.host";
            String PORT = "pg_conn.port";
            String USER = "pg_conn.user";
            String PASSWORD = "pg_conn.password";
            String NAME = "pg_conn.database";
        }
    }

    interface Override{
        String AMOUNT = "type.override";
        String SOURCE_TYPE = "override.type";
        String SINK_TYPE = "override.to";
        String TABLES = "override.tables";
    }
}
