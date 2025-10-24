/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.enums;

import lombok.Getter;
import org.opengauss.config.ApplicationConfig;

/**
 * template config type
 *
 * @since 2025/4/29
 */
@Getter
public enum TemplateConfigType {
    /**
     * migration config template files
     */
    MYSQL_MIGRATION_CONFIG("mysql-migration.properties", FileFormat.PROPERTIES, true, "config",
            "the migration config file with MySQL source database", "mysql-migration-desc.properties"),
    PGSQL_MIGRATION_CONFIG("pgsql-migration.properties", FileFormat.PROPERTIES, true, "config",
            "the migration config file with PostgreSQL source database", "pgsql-migration-desc.properties"),

    /**
     * chameleon config template file
     */
    CHAMELEON_CONFIG("config-example.yml", FileFormat.YML, false, "config/chameleon",
            "the chameleon config file", null),

    /**
     * oG_datasync_full_migration config template file
     */
    FULL_REPLICATE_CONFIG("config.yml", FileFormat.YML, false, "config/full_replicate",
            "the full_replicate config file", null),

    /**
     * datachecker config template files
     */
    DATACHECKER_SINK_CONFIG("application-sink.yml", FileFormat.YML, false, "config/datachecker",
            "the datachecker sink process config file", null),
    DATACHECKER_SOURCE_CONFIG("application-source.yml", FileFormat.YML, false, "config/datachecker",
            "the datachecker source process config file", null),
    DATACHECKER_CHECK_CONFIG("application.yml", FileFormat.YML, false, "config/datachecker",
            "the datachecker check process config file", null),
    DATACHECKER_LOG4J2_CONFIG("log4j2.xml", FileFormat.XML, false, "config/datachecker",
            "the datachecker log4j2 config file", null),

    /**
     * debezium config template files
     */
    DEBEZIUM_CONNECT_AVRO_STANDALONE_CONFIG("connect-avro-standalone.properties", FileFormat.PROPERTIES, false,
            "config/debezium", "the debezium connect standalone config file", null),
    DEBEZIUM_CONNECT_LOG4J2_CONFIG("connect-log4j.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect log4j config file", null),
    DEBEZIUM_CONNECT_MYSQL_SINK_CONFIG("mysql-sink.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect MySQL sink process config file", null),
    DEBEZIUM_CONNECT_MYSQL_SOURCE_CONFIG("mysql-source.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect MySQL source process config file", null),
    DEBEZIUM_CONNECT_OPENGAUSS_SINK_CONFIG("opengauss-sink.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect openGauss sink process config file", null),
    DEBEZIUM_CONNECT_OPENGAUSS_SOURCE_CONFIG("opengauss-source.properties", FileFormat.PROPERTIES, false,
            "config/debezium", "the debezium connect openGauss source process config file", null),
    DEBEZIUM_CONNECT_PGSQL_SINK_CONFIG("postgres-sink.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect PostgreSQL sink process config file", null),
    DEBEZIUM_CONNECT_PGSQL_SOURCE_CONFIG("postgres-source.properties", FileFormat.PROPERTIES, false, "config/debezium",
            "the debezium connect PostgreSQL source process config file", null),
    ;

    TemplateConfigType(String name, FileFormat fileFormat, boolean isInResources, String filePath, String description,
                       String configDescFileName) {
        this.name = name;
        this.fileFormat = fileFormat;
        this.isInResources = isInResources;
        this.filePath = filePath;
        this.description = description;
        this.configDescFileName = configDescFileName;
    }

    private final String name;
    private final FileFormat fileFormat;
    private final boolean isInResources;
    private final String filePath;
    private final String description;
    private final String configDescFileName;

    /**
     * get template config file path
     *
     * @return String file path
     */
    public String getFilePath() {
        if (isInResources) {
            return String.format("%s/%s", filePath, name);
        }

        String templateDirPath = ApplicationConfig.getInstance().getPortalTemplateDirPath();
        return String.format("%s/%s/%s", templateDirPath, filePath, name);
    }

    /**
     * get template config description file path
     *
     * @return String file path
     */
    public String getConfigDescFilePath() {
        if (configDescFileName == null) {
            throw new UnsupportedOperationException("Config file " + name + " does not have config description file");
        }

        if (isInResources) {
            return String.format("%s/%s", filePath, configDescFileName);
        }

        String templateDirPath = ApplicationConfig.getInstance().getPortalTemplateDirPath();
        return String.format("%s/%s/%s", templateDirPath, filePath, configDescFileName);
    }
}
