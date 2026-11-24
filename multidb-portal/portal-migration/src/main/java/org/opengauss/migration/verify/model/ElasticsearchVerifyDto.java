/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.verify.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.opengauss.migration.domain.config.ElasticsearchMigrationConfig;
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

    private final ElasticsearchMigrationConfig migrationConfigDto;

    private RestClient restClient;
    private Connection opengaussConnection;

    public ElasticsearchVerifyDto(ElasticsearchMigrationConfig migrationConfigDto) {
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
