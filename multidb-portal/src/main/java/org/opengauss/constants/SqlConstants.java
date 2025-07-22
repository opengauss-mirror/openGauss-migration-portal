/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

/**
 * Sql constants
 *
 * @since 2025/7/7
 */
public class SqlConstants {
    /**
     * Select version, support MySQL, openGauss, PostgreSQL
     */
    public static final String SELECT_VERSION = "SELECT version();";

    /**
     * Show tables, support openGauss, PostgreSQL
     */
    public static final String SHOW_TABLES = "SELECT tablename FROM pg_tables WHERE SCHEMANAME = ?;";

    /**
     * Check schema exists, support openGauss, PostgreSQL
     */
    public static final String IS_SCHEMA_EXISTS =
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = ?);";

    /**
     * Show variable, support openGauss, PostgreSQL
     */
    public static final String SHOW_VARIABLE = "SHOW %s;";

    /**
     * Count replication slots, support openGauss, PostgreSQL
     */
    public static final String COUNT_REPLICATION_SLOTS = "select count(*) from pg_get_replication_slots();";

    /**
     * Select replication slot names, support openGauss, PostgreSQL
     */
    public static final String SELECT_REPLICATION_SLOT_NAMES = "select slot_name from pg_get_replication_slots();";

    /**
     * Create replication slot, support openGauss, PostgreSQL
     */
    public static final String CREATE_REPLICATION_SLOT = "SELECT * FROM pg_create_logical_replication_slot(?, ?);";

    /**
     * Drop replication slot, support openGauss, PostgreSQL
     */
    public static final String DROP_REPLICATION_SLOT = "SELECT * FROM pg_drop_replication_slot(?);";

    /**
     * Select publication names, support openGauss, PostgreSQL
     */
    public static final String SELECT_PUBLICATION_NAMES = "SELECT pubname from pg_publication;";

    /**
     * Create publication for all tables, support openGauss, PostgreSQL
     */
    public static final String CREATE_PUBLICATION_ALL_TABLES = "CREATE PUBLICATION %s FOR ALL TABLES;";

    /**
     * Create publication for table list, support openGauss, PostgreSQL
     */
    public static final String CREATE_PUBLICATION_FOR_TABLE = "CREATE PUBLICATION %s FOR TABLE %s;";

    /**
     * Drop publication, support openGauss, PostgreSQL
     */
    public static final String DROP_PUBLICATION = "DROP PUBLICATION %s;";

    /**
     * Alter table replica identity full, support openGauss, PostgreSQL
     */
    public static final String ALTER_TABLE_REPLICA_IDENTITY_FULL = "ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY full;";

    /**
     * Alter table replica identity default, support openGauss, PostgreSQL
     */
    public static final String ALTER_TABLE_REPLICA_IDENTITY_DEFAULT =
            "ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY default;";

    /**
     * Is user system admin, support openGauss
     */
    public static final String OPENGAUSS_IS_SYSTEM_ADMIN = "select rolsystemadmin from pg_roles where rolname = ?;";

    /**
     * Is user replication role, support openGauss
     */
    public static final String OPENGAUSS_IS_REPLICATION_ROLE = "select rolreplication from pg_roles where rolname = ?;";

    /**
     * Alter system set, support openGauss
     */
    public static final String OPENGAUSS_ALTER_SYSTEM_SET = "ALTER SYSTEM SET %s TO %s;";

    /**
     * Get database access permissions, support openGauss
     */
    public static final String OPENGAUSS_ACCESS_PERMISSIONS = "select datacl from pg_database where datname = ?;";

    /**
     * Select user auth plugin, support MySQL
     */
    public static final String MYSQL_SELECT_USER_AUTH_PLUGIN = "SELECT USER,PLUGIN FROM mysql.user WHERE USER = ?;";

    /**
     * Show variable, support MySQL
     */
    public static final String MYSQL_SHOW_VARIABLE = "show variables like ?;";

    /**
     * Select user column, support MySQL
     */
    public static final String MYSQL_SELECT_USER_COLUMN = "select %s from mysql.user where user = '%s';";

    /**
     * Show master status, support MySQL
     */
    public static final String MYSQL_SHOW_MASTER_STATUS = "SHOW MASTER STATUS;";

    private SqlConstants() {
    }
}
