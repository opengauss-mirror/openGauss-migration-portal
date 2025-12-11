/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.migration.config.ElasticsearchMigrationConfigDto;
import org.opengauss.domain.migration.config.MilvusMigrationConfigDto;
import org.opengauss.domain.migration.config.MysqlMigrationConfigDto;
import org.opengauss.domain.migration.config.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.exceptions.VerifyException;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.ElasticsearchVerifyDto;
import org.opengauss.migration.verify.model.MilvusVerifyDto;
import org.opengauss.migration.verify.model.MysqlVerifyDto;
import org.opengauss.migration.verify.model.PgsqlVerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.List;

/**
 * Verify manager
 *
 * @since 2025/5/9
 */
public class VerifyManager {
    private static final Logger LOGGER = LogManager.getLogger(VerifyManager.class);

    /**
     * Verify before start MySQL migration
     *
     * @param migrationPhaseList migration phase list
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean mysqlMigrationVerify(
            List<MigrationPhase> migrationPhaseList, MysqlMigrationConfigDto migrationConfigDto,
            TaskWorkspace taskWorkspace) {
        VerifyResult mysqlVerifyResult = new VerifyResult();
        MysqlVerifyDto mysqlVerifyDto = new MysqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getMysqlMigrationVerifyChain(migrationPhaseList).verify(mysqlVerifyDto, mysqlVerifyResult);
        mysqlVerifyDto.closeConnection();
        outputVerifyResult(mysqlVerifyResult, taskWorkspace);
        return mysqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start MySQL reverse phase
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean mysqlReversePhaseVerify(
            MysqlMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace) {
        VerifyResult mysqlVerifyResult = new VerifyResult();
        MysqlVerifyDto mysqlVerifyDto = new MysqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getMysqlReversePhaseVerifyChain().verify(mysqlVerifyDto, mysqlVerifyResult);
        mysqlVerifyDto.closeConnection();
        outputVerifyResult(mysqlVerifyResult, taskWorkspace);
        return mysqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start PostgreSQL migration
     *
     * @param migrationPhaseList migration phase list
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean pgsqlMigrationVerify(
            List<MigrationPhase> migrationPhaseList, PgsqlMigrationConfigDto migrationConfigDto,
            TaskWorkspace taskWorkspace) {
        VerifyResult pgsqlVerifyResult = new VerifyResult();
        PgsqlVerifyDto pgsqlVerifyDto = new PgsqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getPgsqlMigrationVerifyChain(migrationPhaseList).verify(pgsqlVerifyDto, pgsqlVerifyResult);
        pgsqlVerifyDto.closeConnection();
        outputVerifyResult(pgsqlVerifyResult, taskWorkspace);
        return pgsqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start PostgreSQL reverse phase
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean pgsqlReversePhaseVerify(
            PgsqlMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace) {
        VerifyResult pgsqlVerifyResult = new VerifyResult();
        PgsqlVerifyDto pgsqlVerifyDto = new PgsqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getPgsqlReversePhaseVerifyChain().verify(pgsqlVerifyDto, pgsqlVerifyResult);
        pgsqlVerifyDto.closeConnection();
        outputVerifyResult(pgsqlVerifyResult, taskWorkspace);
        return pgsqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start Milvus migration
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean milvusMigrationVerify(
            MilvusMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace
    ) {
        VerifyResult milvusVerifyResult = new VerifyResult();
        MilvusVerifyDto milvusVerifyDto = new MilvusVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getMilvusMigrationVerifyChain().verify(milvusVerifyDto, milvusVerifyResult);
        milvusVerifyDto.closeConnection();
        outputVerifyResult(milvusVerifyResult, taskWorkspace);
        return milvusVerifyResult.isSuccess();
    }

    /**
     * Verify before start Elasticsearch migration
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean elasticsearchMigrationVerify(
            ElasticsearchMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace
    ) {
        VerifyResult verifyResult = new VerifyResult();
        ElasticsearchVerifyDto verifyDto = new ElasticsearchVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getElasticsearchMigrationVerifyChain().verify(verifyDto, verifyResult);
        verifyDto.closeConnection();
        outputVerifyResult(verifyResult, taskWorkspace);
        return verifyResult.isSuccess();
    }

    private static void outputVerifyResult(VerifyResult verifyResult, TaskWorkspace taskWorkspace) {
        String resultFilePath = String.format("%s/%s", taskWorkspace.getStatusDirPath(),
                VerifyConstants.VERIFY_RESULT_FILE_NAME);
        String result = verifyResult.getResult();

        try {
            if (verifyResult.isSuccess()) {
                LOGGER.info("Verify before migration is successful");
            } else {
                LOGGER.error("Verify before migration is failed, following is the detail: {}{}",
                        System.lineSeparator(), result);
            }
            FileUtils.writeToFile(resultFilePath, result, false);
        } catch (IOException e) {
            throw new VerifyException("Failed to write verify result to file: " + resultFilePath, e);
        }
    }
}
