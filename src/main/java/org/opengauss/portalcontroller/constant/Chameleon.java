package org.opengauss.portalcontroller.constant;

import java.util.List;

public interface Chameleon {
    String VENV_PATH = "chameleon.venv.path";
    String PATH = "chameleon.path";
    String PKG_PATH = "chameleon.pkg.path";
    String PKG_NAME = "chameleon.pkg.name";
    String PKG_URL = "chameleon.pkg.url";
    String LOG_PATH = "chameleon.log.path";
    String CONFIG_PATH = "chameleon.config.path";
    String RUNNABLE_FILE_PATH = "chameleon.runnable.file.path";

    interface Parameters {
        interface Mysql {
            String HOST = "sources.mysql.db_conn.host";
            String PORT = "sources.mysql.db_conn.port";
            String USER = "sources.mysql.db_conn.user";
            String PASSWORD = "sources.mysql.db_conn.password";
            String NAME = "sources.mysql.db_conn.database";
            String MAPPING = "sources.mysql.schema_mappings";
        }

        interface Opengauss {
            String HOST = "pg_conn.host";
            String PORT = "pg_conn.port";
            String USER = "pg_conn.user";
            String PASSWORD = "pg_conn.password";
            String NAME = "pg_conn.database";
        }
    }

    interface Override {
        String AMOUNT = "type_override";
        String SOURCE_TYPE = "override_type";
        String SINK_TYPE = "override_to";
        String TABLES = "override_tables";
    }

    interface Order {
        String DROP = "drop_replica_schema";
        String CREATE = "create_replica_schema";
        String ADD = "add_source";
        String INIT = "init_replica";
        String START_TRIGGER = "start_trigger_replica";
        String START_VIEW = "start_view_replica";
        String START_FUNC = "start_func_replica";
        String START_PROC = "start_proc_replica";
        String DETACH = "detach_replica";
        List<String> ALL_ORDER_LIST = List.of(DROP, CREATE, ADD, INIT, START_TRIGGER, START_VIEW, START_FUNC, START_PROC, DETACH);
        List<String> FINAL_ORDER_LIST = List.of(DROP, DETACH);
    }
}
