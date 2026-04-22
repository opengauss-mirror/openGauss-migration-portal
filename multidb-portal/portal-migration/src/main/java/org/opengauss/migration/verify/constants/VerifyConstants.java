/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.constants;

/**
 * Verify constants
 *
 * @since 2025/5/8
 */
public class VerifyConstants {
    /**
     * Verify result file name
     */
    public static final String VERIFY_RESULT_FILE_NAME = "verify.txt";

    /**
     * Verify failed result model
     */
    public static final String VERIFY_FAILED_RESULT_MODEL = "Parameter name: %s, Expected value: %s, Actual value: %s";

    /**
     * Verify occurs SQLException model
     */
    public static final String SQL_EXCEPTION_MODEL =
            "SQL Exception occurred: %s For more details, please check the log file";

    /**
     * Verify occurs other Exception model
     */
    public static final String EXCEPTION_MODEL = "Exception occurred: %s For more details, please check the log file";

    /**
     * MySQL connect user permission: select
     */
    public static final String MYSQL_PERMISSION_SELECT = "select_priv";

    /**
     * MySQL connect user permission: reload
     */
    public static final String MYSQL_PERMISSION_RELOAD = "reload_priv";

    /**
     * MySQL connect user permission: replication client
     */
    public static final String MYSQL_PERMISSION_REP_CLIENT = "repl_client_priv";

    /**
     * MySQL connect user permission: replication slave
     */
    public static final String MYSQL_PERMISSION_REP_SLAVE = "repl_slave_priv";

    /**
     * MySQL connect user permission: lock tables
     */
    public static final String MYSQL_PERMISSION_LOCK_TABLES = "lock_tables_priv";

    /**
     * MySQL connect user permission: insert
     */
    public static final String MYSQL_PERMISSION_INSERT = "insert_priv";

    /**
     * MySQL connect user permission: update
     */
    public static final String MYSQL_PERMISSION_UPDATE = "update_priv";

    /**
     * MySQL connect user permission: delete
     */
    public static final String MYSQL_PERMISSION_DELETE = "delete_priv";

    /**
     * openGauss connect user permission: create
     */
    public static final String OPENGAUSS_PERMISSION_CREATE = "C";

    /**
     * openGauss connect user permission: temporary
     */
    public static final String OPENGAUSS_PERMISSION_TEMPORARY = "T";

    /**
     * openGauss connect user permission: connect
     */
    public static final String OPENGAUSS_PERMISSION_CONNECT = "c";

    /**
     * openGauss connect user permission: alter
     */
    public static final String OPENGAUSS_PERMISSION_ALTER = "A";

    /**
     * openGauss connect user permission: drop
     */
    public static final String OPENGAUSS_PERMISSION_DROP = "P";

    /**
     * openGauss connect user permission: comment
     */
    public static final String OPENGAUSS_PERMISSION_COMMENT = "m";

    /**
     * openGauss connect user permission description
     */
    public static final String OPENGAUSS_PERMISSION_DESC = String.format(
        "openGauss permission description, %s: CREATE, %s: ALTER, %s: DROP, %s: COMMENT, %s: TEMPORARY, %s: CONNECT",
        VerifyConstants.OPENGAUSS_PERMISSION_CREATE, VerifyConstants.OPENGAUSS_PERMISSION_ALTER,
        VerifyConstants.OPENGAUSS_PERMISSION_DROP, VerifyConstants.OPENGAUSS_PERMISSION_COMMENT,
        VerifyConstants.OPENGAUSS_PERMISSION_TEMPORARY, VerifyConstants.OPENGAUSS_PERMISSION_CONNECT);
}
