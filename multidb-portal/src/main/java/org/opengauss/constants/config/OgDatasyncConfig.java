/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * oG_datasync_full_migration config
 *
 * @since 2025/5/29
 */
public class OgDatasyncConfig {
    /**
     * is dump json
     */
    public static final String IS_DUMP_JSON = "isDumpJson";

    /**
     * status dir
     */
    public static final String STATUS_DIR = "statusDir";

    /**
     * openGauss ip
     */
    public static final String OG_CONN_HOST = "ogConn.host";

    /**
     * openGauss port
     */
    public static final String OG_CONN_PORT = "ogConn.port";

    /**
     * openGauss user
     */
    public static final String OG_CONN_USER = "ogConn.user";

    /**
     * openGauss password
     */
    public static final String OG_CONN_PASSWORD = "ogConn.password";

    /**
     * openGauss database
     */
    public static final String OG_CONN_DATABASE = "ogConn.database";

    /**
     * source database host
     */
    public static final String SOURCE_DB_CONN_HOST = "sourceConfig.dbConn.host";

    /**
     * source database port
     */
    public static final String SOURCE_DB_CONN_PORT = "sourceConfig.dbConn.port";

    /**
     * source database user
     */
    public static final String SOURCE_DB_CONN_USER = "sourceConfig.dbConn.user";

    /**
     * source database password
     */
    public static final String SOURCE_DB_CONN_PASSWORD = "sourceConfig.dbConn.password";

    /**
     * source database database
     */
    public static final String SOURCE_DB_CONN_DATABASE = "sourceConfig.dbConn.database";

    /**
     * source database schema mappings
     */
    public static final String SOURCE_SCHEMA_MAPPINGS = "sourceConfig.schemaMappings";

    /**
     * is deleted csv fire when finish
     */
    public static final String IS_DELETE_CSV = "isDeleteCsv";

    /**
     * source csv dir
     */
    public static final String SOURCE_CSV_DIR = "sourceConfig.csvDir";

    /**
     * is record snapshot, default false
     */
    public static final String IS_RECORD_SNAPSHOT = "sourceConfig.isRecordSnapshot";

    /**
     * source database slot name
     */
    public static final String SLOT_NAME = "sourceConfig.slotName";

    /**
     * source database plugin name
     */
    public static final String PLUGIN_NAME = "sourceConfig.pluginName";

    /**
     * enable env password
     */
    public static final String ENABLE_ENV_PASSWORD = "enable.env.password";

    private OgDatasyncConfig() {
    }
}
