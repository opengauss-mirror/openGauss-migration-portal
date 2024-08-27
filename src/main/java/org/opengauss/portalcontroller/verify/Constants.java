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

package org.opengauss.portalcontroller.verify;

/**
 * constants
 *
 * @date :2023/11/15 15:22
 * @description: CommonServiceVerifyChain
 * @version: 1.1
 * @since 1.1
 */
public class Constants {
    /**
     * online
     */
    public static final String MIGRATION_MODE_ONLINE = "2";

    /**
     * offline
     */
    public static final String MIGRATION_MODE_OFFLINE = "1";

    /**
     * check result file name
     */
    public static final String CHECK_RESULT_FILE = "checkResult.json";

    /**
     * string key verify_result_flag
     */
    public static final String KEY_VERIFY_RESULT_FLAG = "verify_result_flag";

    /**
     * string key mysql
     */
    public static final String KEY_MYSQL = "mysql";

    /**
     * mysql encryption
     */
    public static final String ENCRYPTION_MYSQL = "mysql_native_password";

    /**
     * string key opengauss
     */
    public static final String KEY_OPENGAUSS = "opengauss";

    /**
     * string key result
     */
    public static final String KEY_RESULT = "result";

    /**
     * permission is yes
     */
    public static final String PERMISSION_YES = "Y";

    /**
     * string "-"
     */
    public static final String CROSS_BAR = "-";

    /**
     * true
     */
    public static final int KEY_FLAG_TRUE = 0;

    /**
     * false
     */
    public static final int KEY_FLAG_FALSE = 1;

    /**
     * error
     */
    public static final int KEY_FLAG_ERROR = 2;

    /**
     * permission select
     */
    public static final String PERMISSION_SELECT = "select_priv";

    /**
     * permission reload
     */
    public static final String PERMISSION_RELOAD = "reload_priv";

    /**
     * permission replication client
     */
    public static final String PERMISSION_REP_CLIENT = "repl_client_priv";

    /**
     * permission replication slave
     */
    public static final String PERMISSION_REP_SLAVE = "repl_slave_priv";

    /**
     * permission lock tables
     */
    public static final String PERMISSION_LOCK_TABLES = "lock_tables_priv";

    /**
     * permission replication slave
     */
    public static final String PERMISSION_INSERT = "insert_priv";

    /**
     * permission replication slave
     */
    public static final String PERMISSION_UPDATE = "update_priv";

    /**
     * permission replication slave
     */
    public static final String PERMISSION_DELETE = "delete_priv";

    /**
     * openGauss sql model, used to show openGauss guc param
     */
    public static final String SHOW_OPENGAUSS_GUC_PARAM = "show %s;";

    /**
     * MySQL sql model, used to show MySQL system param
     */
    public static final String SHOW_MYSQL_SYSTEM_PARAM = "show variables like '%s';";
}
