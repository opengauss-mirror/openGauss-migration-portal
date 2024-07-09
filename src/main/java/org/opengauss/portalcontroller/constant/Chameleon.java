package org.opengauss.portalcontroller.constant;

import java.util.List;

/**
 * The interface Chameleon.
 */
public interface Chameleon {
    /**
     * The constant VENV_PATH.
     */
    String VENV_PATH = "chameleon.venv.path";

    /**
     * The constant PATH.
     */
    String PATH = "chameleon.path";

    /**
     * The constant INSTALL_PATH.
     */
    String INSTALL_PATH = "chameleon.install.path";

    /**
     * The constant PKG_PATH.
     */
    String PKG_PATH = "chameleon.pkg.path";

    /**
     * The constant PKG_NAME.
     */
    String PKG_NAME = "chameleon.pkg.name";

    /**
     * The constant PKG_URL.
     */
    String PKG_URL = "chameleon.pkg.url";

    /**
     * The constant LOG_PATH.
     */
    String LOG_PATH = "chameleon.log.path";

    /**
     * The constant CONFIG_PATH.
     */
    String CONFIG_PATH = "chameleon.config.path";

    /**
     * The constant RUNNABLE_FILE_PATH.
     */
    String RUNNABLE_FILE_PATH = "chameleon.runnable.file.path";

    /**
     * The interface Parameters.
     */
    interface Parameters {
        /**
         * The interface Mysql.
         */
        interface Mysql {
            /**
             * The constant HOST.
             */
            String HOST = "sources.mysql.db_conn.host";

            /**
             * The constant PORT.
             */
            String PORT = "sources.mysql.db_conn.port";

            /**
             * The constant USER.
             */
            String USER = "sources.mysql.db_conn.user";

            /**
             * The constant PASSWORD.
             */
            String PASSWORD = "sources.mysql.db_conn.password";

            /**
             * The constant NAME.
             */
            String NAME = "sources.mysql.db_conn.database";

            /**
             * The constant MAPPING.
             */
            String MAPPING = "sources.mysql.schema_mappings";

            /**
             * The constant TABLES.
             */
            String TABLES = "sources.mysql.limit_tables";
        }

        /**
         * The interface Opengauss.
         */
        interface Opengauss {
            /**
             * The constant HOST.
             */
            String HOST = "pg_conn.host";

            /**
             * The constant PORT.
             */
            String PORT = "pg_conn.port";

            /**
             * The constant USER.
             */
            String USER = "pg_conn.user";

            /**
             * The constant PASSWORD.
             */
            String PASSWORD = "pg_conn.password";

            /**
             * The constant NAME.
             */
            String NAME = "pg_conn.database";
        }
    }

    /**
     * The interface Override.
     */
    interface Override {
        /**
         * The constant AMOUNT.
         */
        String AMOUNT = "type_override";

        /**
         * The constant SOURCE_TYPE.
         */
        String SOURCE_TYPE = "override_type";

        /**
         * The constant SINK_TYPE.
         */
        String SINK_TYPE = "override_to";

        /**
         * The constant TABLES.
         */
        String TABLES = "override_tables";
    }

    /**
     * The interface Order.
     */
    interface Order {
        /**
         * The constant DROP.
         */
        String DROP = "drop_replica_schema";

        /**
         * The constant CREATE.
         */
        String CREATE = "create_replica_schema";

        /**
         * The constant ADD.
         */
        String ADD = "add_source";

        /**
         * The constant INIT.
         */
        String INIT = "init_replica";

        /**
         * The constant START_TRIGGER.
         */
        String START_TRIGGER = "start_trigger_replica";

        /**
         * The constant START_VIEW.
         */
        String START_VIEW = "start_view_replica";

        /**
         * The constant START_FUNC.
         */
        String START_FUNC = "start_func_replica";

        /**
         * The constant START_PROC.
         */
        String START_PROC = "start_proc_replica";

        /**
         * The constant DETACH.
         */
        String DETACH = "detach_replica";

        /**
         * The All order list.
         */
        List<String> ALL_ORDER_LIST = List.of(DROP, CREATE, ADD, INIT, START_TRIGGER, START_VIEW, START_FUNC,
                START_PROC, DETACH);

        /**
         * The Final order list.
         */
        List<String> FINAL_ORDER_LIST = List.of(DROP, DETACH);
    }
}
