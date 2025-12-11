/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.opengauss.domain.migration.config.ElasticsearchMigrationConfigDto;
import org.opengauss.enums.DatabaseType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * ElasticsearchVerifyChain
 *
 * @since 2025/10/31
 */
@Getter
@Setter
public class ElasticsearchVerifyDto extends AbstractVerifyDto {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchVerifyDto.class);

    private final ElasticsearchMigrationConfigDto migrationConfigDto;

    private RestClient restClient;
    private Connection opengaussConnection;

    public ElasticsearchVerifyDto(ElasticsearchMigrationConfigDto migrationConfigDto) {
        this.migrationConfigDto = migrationConfigDto;
    }

    public RestClient getRestClient() {
        if (restClient == null) {
            throw new IllegalStateException("Elasticsearch Connect Verify has not been performed");
        }
        return restClient;
    }

    @Override
    public Connection getOpengaussConnection() {
        if (opengaussConnection == null) {
            throw new IllegalStateException("OpenGauss Connect Verify has not been performed");
        }
        return opengaussConnection;
    }

    @Override
    public DatabaseType getSourceDbType() {
        return DatabaseType.ELASTICSEARCH;
    }

    @Override
    public void closeConnection() {
        try {
            if (restClient != null) {
                restClient.close();
            }

            if (opengaussConnection != null) {
                opengaussConnection.close();
            }
        } catch (IOException e) {
            LOGGER.trace("Failed to close Elasticsearch connection", e);
        } catch (SQLException e) {
            LOGGER.trace("Failed to close openGauss connection", e);
        }
    }
}
