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

import io.milvus.v2.client.MilvusClientV2;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.migration.domain.config.MilvusMigrationConfig;
import org.opengauss.enums.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MilvusVerifyDto
 *
 * @since 2025/10/31
 */
@Getter
@Setter
public class MilvusVerifyDto extends AbstractVerifyDto {
    private static final Logger LOGGER = LogManager.getLogger(MilvusVerifyDto.class);

    private final MilvusMigrationConfig migrationConfigDto;

    private MilvusClientV2 milvusClientV2;
    private Connection opengaussConnection;

    public MilvusVerifyDto(MilvusMigrationConfig migrationConfigDto) {
        this.migrationConfigDto = migrationConfigDto;
    }

    public MilvusClientV2 getMilvusClientV2() {
        if (milvusClientV2 == null) {
            throw new IllegalStateException("Milvus Connect Verify has not been performed");
        }
        return milvusClientV2;
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
        return DatabaseType.MILVUS;
    }

    @Override
    public void closeConnection() {
        if (milvusClientV2 != null) {
            milvusClientV2.close();
        }
        if (opengaussConnection != null) {
            try {
                opengaussConnection.close();
            } catch (SQLException e) {
                LOGGER.trace("Failed to close openGauss connection", e);
            }
        }
    }
}
