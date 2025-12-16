/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.config;

/**
 * MilvusMigrationToolConfig
 *
 * @since 2025/10/23
 */
public class MilvusMigrationToolConfig {
    /**
     * Milvus server IP address
     */
    public static final String MILVUS_IP = "Milvus.host";

    /**
     * Milvus server port
     */
    public static final String MILVUS_PORT = "Milvus.port";

    /**
     * Milvus username, if not set, use anonymous access. Not supported yet.
     */
    public static final String MILVUS_USERNAME = "Milvus.username";

    /**
     * Milvus password, if not set, use anonymous access. Not supported yet.
     */
    public static final String MILVUS_PASSWORD = "Milvus.password";

    /**
     * openGauss server IP address
     */
    public static final String OPENGAUSS_IP = "openGauss.host";

    /**
     * openGauss username
     */
    public static final String OPENGAUSS_USER = "openGauss.user";

    /**
     * openGauss user password
     */
    public static final String OPENGAUSS_PASSWORD = "openGauss.password";

    /**
     * openGauss server port
     */
    public static final String OPENGAUSS_PORT = "openGauss.port";

    /**
     * openGauss database name
     */
    public static final String OPENGAUSS_DATABASE = "openGauss.database";

    /**
     * Milvus collection name
     */
    public static final String TABLE_MILVUS_COLLECTION_NAME = "Table.milvus_collection_name";

    /**
     * openGauss table name
     */
    public static final String TABLE_OPENGUASS_TABLE_NAME = "Table.opengauss_table_name";

    /**
     * Sparse vector default dimension
     */
    public static final String SPARSE_VECTOR_DEFAULT_DIMENSION = "SparseVector.default_dimension";

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

    private MilvusMigrationToolConfig() {
    }
}
