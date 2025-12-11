/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify.model;

import io.milvus.v2.client.MilvusClientV2;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
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

    private final MilvusMigrationConfigDto migrationConfigDto;

    private MilvusClientV2 milvusClientV2;
    private Connection opengaussConnection;

    public MilvusVerifyDto(MilvusMigrationConfigDto migrationConfigDto) {
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
