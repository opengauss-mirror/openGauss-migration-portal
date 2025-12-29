/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * ElasticsearchMigrationToolConfig
 *
 * @since 2025/10/23
 */
public class ElasticsearchMigrationToolConfig {
    /**
     * Elasticsearch host, e.g. http://localhost:9200
     */
    public static final String ELASTICSEARCH_HOST = "Elasticsearch.host";

    /**
     * Elasticsearch username, if not set, use anonymous access. Not supported yet.
     */
    public static final String ELASTICSEARCH_USERNAME = "Elasticsearch.username";

    /**
     * Elasticsearch password, if not set, use anonymous access. Not supported yet.
     */
    public static final String ELASTICSEARCH_PASSWORD = "Elasticsearch.password";

    /**
     * openGauss server IP address
     */
    public static final String OPENGAUSS_IP = "openGauss.ip";

    /**
     * openGauss server port
     */
    public static final String OPENGAUSS_PORT = "openGauss.port";

    /**
     * openGauss database name
     */
    public static final String OPENGAUSS_DATABASE = "openGauss.database";

    /**
     * openGauss username
     */
    public static final String OPENGAUSS_USERNAME = "openGauss.username";

    /**
     * openGauss password
     */
    public static final String OPENGAUSS_PASSWORD = "openGauss.password";

    /**
     * Elasticsearch index name
     */
    public static final String TABLE_ELASTICSEARCH_INDEX_NAME = "Table.elasticsearch_index_name";

    /**
     * openGauss table name, default the same as the index name
     */
    public static final String TABLE_OPENGAUSS_TABLE_NAME = "Table.opengauss_table_name";

    /**
     * Output folder path
     */
    public static final String OUTPUT_FOLDER = "Output.folder";

    /**
     * Whether to clean up temporary files after migration
     */
    public static final String MIGRATION_CLEANUP_TEMP_FILES = "Migration.cleanup_temp_files";

    /**
     * Enable env password
     */
    public static final String ENABLE_ENV_PASSWORD = "enable.env.password";

    private ElasticsearchMigrationToolConfig() {
    }
}
